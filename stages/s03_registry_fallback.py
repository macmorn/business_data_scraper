"""Stage 3: Fallback registry lookups for companies not found on Northdata."""

from __future__ import annotations

import json
import logging

import db
from clients import opencorporates, gleif, pappers, brave_search
from models import STAGE_PENDING_FALLBACK, STAGE_PENDING_CEO
from utils.logging_setup import ProgressTracker

logger = logging.getLogger(__name__)


async def run() -> None:
    """Process all companies pending fallback lookups."""
    companies = db.get_pending(STAGE_PENDING_FALLBACK, limit=10000)
    if not companies:
        logger.info("Stage 3: No companies pending fallback lookup")
        return

    logger.info("=" * 40)
    logger.info("Stage 3: Registry Fallback (%d companies)", len(companies))
    logger.info("=" * 40)

    tracker = ProgressTracker(len(companies), "fallback")
    results = {"found": 0, "partial": 0, "not_found": 0, "error": 0}

    for company in companies:
        try:
            data = None
            source = None

            # Try sources in priority order, stop when we get a good match
            # 1. OpenCorporates
            if not data:
                try:
                    oc_result = await opencorporates.search_company_eu(company.name_original)
                    if oc_result:
                        company.opencorporates_raw = json.dumps(oc_result, ensure_ascii=False)
                        data = oc_result
                        source = "opencorporates"
                except Exception as e:
                    logger.debug("OpenCorporates error for '%s': %s", company.name_original, e)

            # 2. GLEIF
            if not data:
                try:
                    gleif_result = await gleif.search_company(company.name_original)
                    if gleif_result:
                        company.gleif_raw = json.dumps(gleif_result, ensure_ascii=False)
                        data = gleif_result
                        source = "gleif"
                except Exception as e:
                    logger.debug("GLEIF error for '%s': %s", company.name_original, e)

            # 3. Pappers (French companies)
            if not data:
                try:
                    pappers_result = await pappers.search_company(company.name_original)
                    if pappers_result:
                        company.pappers_raw = json.dumps(pappers_result, ensure_ascii=False)
                        data = pappers_result
                        source = "pappers"
                except Exception as e:
                    logger.debug("Pappers error for '%s': %s", company.name_original, e)

            # 4. Brave Search (last resort)
            if not data:
                try:
                    brave_result = await brave_search.search_company(company.name_original)
                    if brave_result:
                        company.brave_raw = json.dumps(brave_result, ensure_ascii=False)
                        data = brave_result
                        source = "brave_search"
                except Exception as e:
                    logger.debug("Brave error for '%s': %s", company.name_original, e)

            # Apply results
            if data:
                _apply_fallback_data(company, data, source)
                has_key_fields = company.matched_name and company.country
                if has_key_fields:
                    results["found"] += 1
                    tracker.tick(company.name_original, f"found via {source}")
                else:
                    results["partial"] += 1
                    company.needs_review_flag = True
                    tracker.tick(company.name_original, f"partial via {source}")
            else:
                results["not_found"] += 1
                company.needs_review_flag = True
                tracker.tick(company.name_original, "not found in any source")

            company.stage = STAGE_PENDING_CEO
            db.update_company(company)

        except Exception as e:
            logger.error("Unexpected error for '%s': %s", company.name_original, e)
            db.mark_failed(company.id, str(e))
            results["error"] += 1

    tracker.summary(results)


def _apply_fallback_data(company, data: dict, source: str) -> None:
    """Apply fallback source data to a CompanyRecord, only filling empty fields."""
    if not company.matched_name:
        company.matched_name = data.get("name") or company.name_original
    if not company.country:
        company.country = data.get("country")
    if not company.legal_form:
        company.legal_form = data.get("legal_form")
    if not company.status:
        company.status = data.get("status")
    if not company.founded_year:
        company.founded_year = data.get("founded_year")
    if not company.address:
        company.address = data.get("address")
    if not company.employees_range:
        company.employees_range = data.get("employees_range")
    if not company.revenue_range:
        company.revenue_range = data.get("revenue_range")
    if not company.last_accounts_year:
        company.last_accounts_year = data.get("last_accounts_year")

    # Merge officers
    new_officers = data.get("officers", [])
    if new_officers and not company.officers:
        company.officers = json.dumps(new_officers, ensure_ascii=False)

    # Track data sources
    existing = company.data_sources_used or ""
    sources = [s.strip() for s in existing.split(",") if s.strip()]
    if source not in sources:
        sources.append(source)
    company.data_sources_used = ",".join(sources)
