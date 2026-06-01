"""Stage 5: AI enrichment - disambiguation + career summaries.

Uses Claude API for:
1. Resolving multi-match Northdata results (companies with stage=pending_ai
   that have northdata_raw with multiple candidates)
2. Generating CEO career summaries for companies with CEO data
"""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path

import cache
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

    _COMPANY_TIMEOUT = 600  # 10 min safety net per company
    source_run = Path(config.INPUT_PDF).stem

    try:
        for company in companies:
            try:
                await asyncio.wait_for(
                    _enrich_company(company, clie=client, rate_limiter=rate_limiter, results=results),
                    timeout=_COMPANY_TIMEOUT,
                )

                company.stage = STAGE_PENDING_NORMALIZE
                db.update_company(company)

                # Write to global enrichment cache so other runs can reuse
                try:
                    cache.store(company, source_run)
                except Exception as e:
                    logger.warning("Failed to cache '%s': %s", company.name_original, e)

                action = []
                if company.ceo_career_summary:
                    action.append("summary")
                if company.ceo_linkedin_url:
                    action.append("linkedin")
                if company.revenue or company.employees_count:
                    action.append("financials")
                tracker.tick(company.name_original, ", ".join(action) if action else "passed through")

            except claude_ai.ClaudeUsageLimitError as e:
                logger.error(
                    "Usage limit hit at '%s' (subtype=%s) — stopping Stage 5; "
                    "remaining companies stay at pending_ai for rerun",
                    company.name_original, e.subtype,
                )
                db.mark_for_rerun(
                    company.id, f"usage_limit_reached:{e.subtype}", STAGE_PENDING_AI
                )
                results["error"] += 1
                break
            except asyncio.TimeoutError:
                logger.error(
                    "Company '%s' enrichment timed out after %ds, moving on",
                    company.name_original, _COMPANY_TIMEOUT,
                )
                db.mark_failed(company.id, f"enrichment_timeout_{_COMPANY_TIMEOUT}s")
                results["error"] += 1
            except Exception as e:
                logger.error("AI enrichment error for '%s': %s", company.name_original, e)
                db.mark_failed(company.id, str(e))
                results["error"] += 1
    finally:
        if client:
            await client.stop()

    tracker.summary(results)


async def _enrich_company(
    company,
    *,
    clie: NorthdataClient | None,
    rate_limiter: RateLimiter | None,
    results: dict,
) -> None:
    """Run all enrichment tasks for a single company."""
    # Task 1: Disambiguation (if northdata returned multiple matches)
    if company.northdata_raw:
        raw = json.loads(company.northdata_raw)
        if raw.get("status") == "multiple" and raw.get("matches"):
            await _disambiguate_company(company, raw["matches"], clie, rate_limiter)
            results["disambiguated"] += 1

    # Single merged enrichment call: CEO + financials + business description.
    # Replaces the former research_ceo/discover_ceo + enrich_missing_financials
    # + estimate_employee_count + summarize_corporate_structure chain.
    cname = company.matched_name or company.name_original

    had_ceo_name = bool(company.ceo_name)

    try:
        data = await claude_ai.enrich_company(
            company_name=cname,
            country=company.country,
            legal_form=company.legal_form,
            known_ceo_name=company.ceo_name,
            known_ceo_title=company.ceo_current_title,
            known_revenue=company.revenue,
            known_employees=company.employees_count or company.employees_range,
        )
    except claude_ai.ClaudeUsageLimitError:
        raise
    except Exception as e:
        logger.warning("Merged enrichment failed for '%s': %s", company.name_original, e)
        return

    ceo = data.get("ceo") or {}
    fin = data.get("financials") or {}

    # --- Apply CEO ---
    if had_ceo_name:
        # We already had a name (from registry/structure stages): only enrich.
        if ceo.get("career_summary"):
            company.ceo_career_summary = ceo["career_summary"]
        if ceo.get("linkedin_url"):
            company.ceo_linkedin_url = ceo["linkedin_url"]
        if company.ceo_career_summary:
            results["summary_generated"] += 1
    elif ceo.get("name"):
        # Newly discovered CEO.
        company.ceo_name = ceo["name"]
        company.ceo_current_title = ceo.get("title") or "Managing Director"
        company.ceo_career_summary = ceo.get("career_summary")
        company.ceo_linkedin_url = ceo.get("linkedin_url")
        company.ceo_confidence = "medium"
        results["ceo_discovered"] += 1
        logger.info("  Discovered CEO for '%s': %s", company.name_original, company.ceo_name)

    # --- Apply financials (only fill gaps) ---
    filled_financials = False
    if fin.get("revenue") and not company.revenue:
        company.revenue = fin["revenue"]
        if not company.revenue_range:
            company.revenue_range = fin["revenue"]
        filled_financials = True
    if fin.get("employees_count") and not company.employees_count:
        company.employees_count = fin["employees_count"]
        if not company.employees_range:
            company.employees_range = fin["employees_count"]
        filled_financials = True
    if fin.get("total_assets") and not company.total_assets:
        company.total_assets = fin["total_assets"]
        filled_financials = True
    if filled_financials:
        _append_source(company, "claude_web")
        results["financials_enriched"] += 1

    # --- Apply business description -> corporate structure summary (gap-fill) ---
    # S04b may have already written a richer related-entity summary; don't clobber it.
    if not company.corporate_structure_summary and data.get("business_description"):
        company.corporate_structure_summary = data["business_description"]


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
