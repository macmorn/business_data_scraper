"""Stage 5: AI enrichment - disambiguation + career summaries.

Uses Claude API for:
1. Resolving multi-match Northdata results (companies with stage=pending_ai
   that have northdata_raw with multiple candidates)
2. Generating CEO career summaries for companies with CEO data
"""

from __future__ import annotations

import json
import logging

import config
import db
from clients import claude_ai
from clients.northdata_browser import NorthdataClient
from models import STAGE_PENDING_AI, STAGE_PENDING_NORMALIZE, STAGE_PENDING_CEO
from stages.s02_northdata import apply_company_data
from stages.s04_ceo_lookup import _extract_ceo_from_officers
from utils.logging_setup import ProgressTracker
from utils.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)


def _append_source(company, source: str) -> None:
    """Append a data source tag if not already present."""
    sources = company.data_sources_used or ""
    if source not in sources:
        company.data_sources_used = f"{sources},{source}" if sources else source


async def run() -> None:
    """Process all companies pending AI enrichment."""
    companies = db.get_pending(STAGE_PENDING_AI, limit=10000)
    if not companies:
        logger.info("Stage 5: No companies pending AI enrichment")
        return

    logger.info("=" * 40)
    logger.info("Stage 5: AI Enrichment (%d companies)", len(companies))
    logger.info("=" * 40)

    tracker = ProgressTracker(len(companies), "ai_enrich")
    results = {
        "disambiguated": 0, "summary_generated": 0, "ceo_discovered": 0,
        "financials_enriched": 0, "skipped": 0, "error": 0,
    }

    # Check if any companies need northdata scraping (disambiguation with URL follow-up)
    needs_scraping = any(
        c.northdata_raw and '"status": "multiple"' in c.northdata_raw
        for c in companies
    )

    client = None
    rate_limiter = None
    if needs_scraping:
        client = NorthdataClient(
            email=config.NORTHDATA_EMAIL,
            password=config.NORTHDATA_PASSWORD,
            retry_attempts=config.NORTHDATA_RETRY_ATTEMPTS,
        )
        await client.start()
        rate_limiter = RateLimiter(config.NORTHDATA_DELAY_MIN, config.NORTHDATA_DELAY_MAX)

    try:
        for company in companies:
            try:
                # Task 1: Disambiguation (if northdata returned multiple matches)
                if company.northdata_raw:
                    raw = json.loads(company.northdata_raw)
                    if raw.get("status") == "multiple" and raw.get("matches"):
                        await _disambiguate_company(company, raw["matches"], client, rate_limiter)
                        results["disambiguated"] += 1

                # Task 2: CEO research or discovery
                if company.ceo_name and company.ceo_current_title:
                    # Known CEO — research career + LinkedIn
                    try:
                        ceo_data = await claude_ai.research_ceo(
                            ceo_name=company.ceo_name,
                            ceo_title=company.ceo_current_title,
                            company_name=company.matched_name or company.name_original,
                        )
                        company.ceo_career_summary = ceo_data.get("career_summary")
                        company.ceo_linkedin_url = ceo_data.get("linkedin_url")
                        results["summary_generated"] += 1
                    except Exception as e:
                        logger.warning("CEO research failed for %s: %s", company.ceo_name, e)
                else:
                    # No CEO found — discover via web search
                    try:
                        ceo_data = await claude_ai.discover_ceo(
                            company_name=company.matched_name or company.name_original,
                            country=company.country,
                            legal_form=company.legal_form,
                        )
                        if ceo_data and ceo_data.get("name"):
                            company.ceo_name = ceo_data["name"]
                            company.ceo_current_title = ceo_data.get("title", "Managing Director")
                            company.ceo_career_summary = ceo_data.get("career_summary")
                            company.ceo_linkedin_url = ceo_data.get("linkedin_url")
                            company.ceo_confidence = "medium"
                            results["ceo_discovered"] += 1
                            logger.info(
                                "  Discovered CEO for '%s': %s",
                                company.name_original, company.ceo_name,
                            )
                    except Exception as e:
                        logger.warning(
                            "CEO discovery failed for '%s': %s",
                            company.name_original, e,
                        )

                # Task 3: Enrich missing revenue via web search
                if not company.revenue:
                    try:
                        existing = {}
                        if company.employees_count:
                            existing["employees_count"] = company.employees_count
                        if company.employees_range:
                            existing["employees_range"] = company.employees_range

                        fin_data = await claude_ai.enrich_missing_financials(
                            company_name=company.matched_name or company.name_original,
                            country=company.country,
                            existing_data=existing if existing else None,
                        )
                        if fin_data.get("employees_count") and not company.employees_count:
                            company.employees_count = fin_data["employees_count"]
                            if not company.employees_range:
                                company.employees_range = fin_data["employees_count"]
                        if fin_data.get("revenue"):
                            company.revenue = fin_data["revenue"]
                            if not company.revenue_range:
                                company.revenue_range = fin_data["revenue"]
                        if fin_data.get("total_assets"):
                            company.total_assets = fin_data["total_assets"]

                        _append_source(company, "claude_web")
                        results["financials_enriched"] += 1
                    except Exception as e:
                        logger.warning(
                            "Financial enrichment failed for '%s': %s",
                            company.name_original, e,
                        )

                # Task 4: Estimate employee count if still missing
                if not company.employees_count:
                    try:
                        emp_count = await claude_ai.estimate_employee_count(
                            company_name=company.matched_name or company.name_original,
                            country=company.country,
                            revenue=company.revenue,
                        )
                        if emp_count:
                            company.employees_count = emp_count
                            if not company.employees_range:
                                company.employees_range = emp_count
                            _append_source(company, "claude_web")
                            logger.info(
                                "  Estimated employees for '%s': %s",
                                company.name_original, emp_count,
                            )
                    except Exception as e:
                        logger.warning(
                            "Employee estimation failed for '%s': %s",
                            company.name_original, e,
                        )

                # Task 5: Generate corporate structure summary if missing
                if not company.corporate_structure_summary:
                    try:
                        summary = await claude_ai.summarize_corporate_structure(
                            company_name=company.matched_name or company.name_original,
                            legal_form=company.legal_form,
                            country=company.country,
                            revenue=company.revenue,
                            employees=company.employees_count or company.employees_range,
                            ceo_name=company.ceo_name,
                            ceo_title=company.ceo_current_title,
                        )
                        if summary:
                            company.corporate_structure_summary = summary
                    except Exception as e:
                        logger.warning(
                            "Structure summary failed for '%s': %s",
                            company.name_original, e,
                        )

                company.stage = STAGE_PENDING_NORMALIZE
                db.update_company(company)

                action = []
                if company.ceo_career_summary:
                    action.append("summary")
                if company.ceo_linkedin_url:
                    action.append("linkedin")
                if company.revenue or company.employees_count:
                    action.append("financials")
                tracker.tick(company.name_original, ", ".join(action) if action else "passed through")

            except Exception as e:
                logger.error("AI enrichment error for '%s': %s", company.name_original, e)
                db.mark_failed(company.id, str(e))
                results["error"] += 1
    finally:
        if client:
            await client.stop()

    tracker.summary(results)


async def _disambiguate_company(
    company,
    matches: list[dict],
    client: NorthdataClient | None,
    rate_limiter: RateLimiter | None,
) -> None:
    """Use Claude to pick the best match, then scrape the chosen company's page."""
    context_hints = {}
    if company.country:
        context_hints["country_hint"] = company.country
    if company.address:
        context_hints["address_hint"] = company.address

    result = await claude_ai.disambiguate(
        original_name=company.name_original,
        candidates=matches,
        context_hints=context_hints if context_hints else None,
    )

    idx = result.get("index", -1)
    confidence = result.get("confidence", 0.0)

    if idx >= 0 and idx < len(matches):
        chosen = matches[idx]
        company.matched_name = chosen.get("name")
        company.confidence_score = confidence
        if confidence < 0.7:
            company.needs_review_flag = True
        logger.info(
            "Disambiguated '%s' -> '%s' (confidence=%.2f)",
            company.name_original, company.matched_name, confidence,
        )

        # Scrape the chosen company's northdata page for full data
        url = chosen.get("url", "")
        if url and client:
            if not url.startswith("http"):
                url = f"https://www.northdata.com{url}"
            if rate_limiter:
                await rate_limiter.wait()
            try:
                scraped_data = await client.scrape_company_url(url)
                apply_company_data(company, scraped_data)

                # Update northdata_raw with scraped data for caching
                raw = json.loads(company.northdata_raw) if company.northdata_raw else {}
                raw["scraped_data"] = scraped_data
                raw["disambiguation"] = {"index": idx, "confidence": confidence}
                company.northdata_raw = json.dumps(raw, ensure_ascii=False)

                # Extract CEO since these companies skipped stage 4
                if not company.ceo_name and company.officers:
                    ceo = _extract_ceo_from_officers(company.officers)
                    if ceo:
                        company.ceo_name = ceo["name"]
                        company.ceo_current_title = ceo["role"]
                        company.ceo_confidence = "high"

                logger.info(
                    "  Scraped disambiguated '%s' → %s | revenue=%s | employees=%s",
                    company.name_original, company.matched_name,
                    company.revenue or "?", company.employees_count or "?",
                )
            except Exception as e:
                logger.warning(
                    "Failed to scrape disambiguated URL for '%s': %s",
                    company.name_original, e,
                )
        elif not url:
            logger.warning("No URL for disambiguated match of '%s'", company.name_original)
    else:
        company.needs_review_flag = True
        company.confidence_score = 0.0
        logger.info("Could not disambiguate '%s' - flagged for review", company.name_original)
