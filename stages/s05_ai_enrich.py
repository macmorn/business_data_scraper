"""Stage 5: AI enrichment - disambiguation of multi-match results + career summaries."""

from __future__ import annotations

import json
import logging

import db
from clients import claude_ai
from clients.northdata_browser import NorthdataClient
from models import STAGE_PENDING_AI, STAGE_PENDING_NORMALIZE, STAGE_PENDING_CEO
from utils.logging_setup import ProgressTracker
from utils.rate_limiter import RateLimiter
import config

logger = logging.getLogger(__name__)


async def run() -> None:
    """Process companies needing AI disambiguation and generate career summaries."""
    companies = db.get_pending(STAGE_PENDING_AI, limit=10000)
    if not companies:
        logger.info("Stage 5: No companies pending AI enrichment")
        return

    logger.info("=" * 40)
    logger.info("Stage 5: AI Enrichment (%d companies)", len(companies))
    logger.info("=" * 40)

    tracker = ProgressTracker(len(companies), "ai_enrich")
    results = {"disambiguated": 0, "summary_generated": 0, "unresolved": 0, "error": 0}

    # Split companies into those needing disambiguation vs just summaries
    needs_disambiguation = []
    needs_summary_only = []

    for company in companies:
        if company.northdata_raw:
            try:
                nd = json.loads(company.northdata_raw)
                if nd.get("status") == "multiple":
                    needs_disambiguation.append(company)
                    continue
            except (json.JSONDecodeError, TypeError):
                pass
        needs_summary_only.append(company)

    # Phase 1: Disambiguation
    if needs_disambiguation:
        logger.info("Disambiguating %d multi-match companies", len(needs_disambiguation))
        northdata_client = None
        rate_limiter = RateLimiter(config.NORTHDATA_DELAY_SECONDS)

        try:
            for company in needs_disambiguation:
                try:
                    nd = json.loads(company.northdata_raw)
                    candidates = nd.get("matches", [])

                    ai_result = await claude_ai.disambiguate_company(
                        company.name_original, candidates
                    )

                    if ai_result["index"] >= 0 and ai_result["confidence"] >= 0.5:
                        best = candidates[ai_result["index"]]
                        company.matched_name = best.get("name", company.name_original)
                        company.confidence_score = ai_result["confidence"]

                        # If we have a URL, scrape the detail page
                        url = best.get("url")
                        if url:
                            if not northdata_client:
                                northdata_client = NorthdataClient()
                                await northdata_client.start()
                            await rate_limiter.wait()
                            data = await northdata_client.scrape_company_url(
                                url if url.startswith("http") else f"https://www.northdata.com{url}"
                            )
                            _apply_scraped_data(company, data)

                        company.needs_review_flag = ai_result["confidence"] < 0.8
                        company.data_sources_used = _add_source(company.data_sources_used, "northdata")
                        results["disambiguated"] += 1
                        tracker.tick(company.name_original, f"disambiguated (conf={ai_result['confidence']:.2f})")
                    else:
                        company.needs_review_flag = True
                        results["unresolved"] += 1
                        tracker.tick(company.name_original, "unresolved")

                    # Move to CEO lookup stage (they haven't been through it yet)
                    company.stage = STAGE_PENDING_CEO
                    db.update_company(company)

                except Exception as e:
                    logger.error("Disambiguation error for '%s': %s", company.name_original, e)
                    db.mark_failed(company.id, str(e))
                    results["error"] += 1

        finally:
            if northdata_client:
                await northdata_client.stop()

    # Phase 2: Career summaries for companies that already have CEO info
    for company in needs_summary_only:
        try:
            if company.ceo_name and not company.ceo_career_summary:
                summary = await claude_ai.generate_career_summary(
                    ceo_name=company.ceo_name,
                    company_name=company.matched_name or company.name_original,
                    title=company.ceo_current_title,
                )
                if summary:
                    company.ceo_career_summary = summary
                    results["summary_generated"] += 1
                    tracker.tick(company.name_original, "summary generated")
                else:
                    tracker.tick(company.name_original, "summary failed")
            else:
                tracker.tick(company.name_original, "no CEO for summary")

            company.stage = STAGE_PENDING_NORMALIZE
            db.update_company(company)

        except Exception as e:
            logger.error("Career summary error for '%s': %s", company.name_original, e)
            db.mark_failed(company.id, str(e))
            results["error"] += 1

    tracker.summary(results)


def _apply_scraped_data(company, data: dict) -> None:
    """Apply scraped Northdata detail page data to company record."""
    if not company.matched_name:
        company.matched_name = data.get("name")
    if not company.legal_form:
        company.legal_form = data.get("legal_form")
    if not company.status:
        company.status = data.get("status")
    if not company.address:
        company.address = data.get("address")
    if not company.founded_year:
        company.founded_year = data.get("founded_year")
    if not company.employees_range:
        company.employees_range = data.get("employees_range")
    if not company.revenue_range:
        company.revenue_range = data.get("revenue_range")
    if not company.last_accounts_year:
        company.last_accounts_year = data.get("last_accounts_year")

    officers = data.get("officers", [])
    if officers and not company.officers:
        company.officers = json.dumps(officers, ensure_ascii=False)

    if company.address and not company.country:
        from stages.s02_northdata import _guess_country
        company.country = _guess_country(company.address)


def _add_source(existing: str | None, new_source: str) -> str:
    """Add a data source to the comma-separated list."""
    sources = [s.strip() for s in (existing or "").split(",") if s.strip()]
    if new_source not in sources:
        sources.append(new_source)
    return ",".join(sources)
