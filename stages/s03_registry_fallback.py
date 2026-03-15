"""Stage 3: Registry fallback for companies not found on Northdata.

Tries sources in priority order: OpenCorporates -> GLEIF -> Pappers -> Brave.
Stops as soon as a high-confidence match is found.
"""

from __future__ import annotations

import json
import logging

import db
from clients import opencorporates, gleif, pappers, brave_search
from models import STAGE_PENDING_FALLBACK, STAGE_PENDING_CEO
from utils.logging_setup import ProgressTracker

logger = logging.getLogger(__name__)


async def run() -> None:
    """Process all companies pending registry fallback."""
    companies = db.get_pending(STAGE_PENDING_FALLBACK, limit=10000)
    if not companies:
        logger.info("Stage 3: No companies pending registry fallback")
        return

    logger.info("=" * 40)
    logger.info("Stage 3: Registry Fallback (%d companies)", len(companies))
    logger.info("=" * 40)

    tracker = ProgressTracker(len(companies), "registry_fallback")
    results = {"found": 0, "partial": 0, "not_found": 0, "error": 0}

    for company in companies:
        try:
            data, source = await _try_all_sources(company.name_original, company.country)

            if data:
                _apply_fallback_data(company, data, source)
                quality = "found" if company.matched_name else "partial"
                results[quality] += 1
                tracker.tick(company.name_original, f"{quality} via {source}")
            else:
                company.needs_review_flag = True
                results["not_found"] += 1
                tracker.tick(company.name_original, "not found in any source")

            company.stage = STAGE_PENDING_CEO
            db.update_company(company)

        except Exception as e:
            logger.error("Fallback error for '%s': %s", company.name_original, e)
            db.mark_failed(company.id, str(e))
            results["error"] += 1

    tracker.summary(results)


async def _try_all_sources(name: str, country: str | None) -> tuple[dict | None, str]:
    """Try each registry source in priority order. Return (data, source_name)."""

    # 1. OpenCorporates
    try:
        data = await opencorporates.search_company_eu(name)
        if data and data.get("name"):
            return data, "opencorporates"
    except Exception as e:
        logger.warning("OpenCorporates failed for '%s': %s", name, e)

    # 2. GLEIF
    try:
        data = await gleif.search_company(name)
        if data and data.get("name"):
            return data, "gleif"
    except Exception as e:
        logger.warning("GLEIF failed for '%s': %s", name, e)

    # 3. Pappers (only for French companies or if country unknown)
    if not country or country == "FR":
        try:
            data = await pappers.search_company(name)
            if data and data.get("name"):
                return data, "pappers"
        except Exception as e:
            logger.warning("Pappers failed for '%s': %s", name, e)

    # 4. Brave Search (last resort)
    try:
        data = await brave_search.search_company(name)
        if data and (data.get("country") or data.get("founded_year")):
            return data, "brave_search"
    except Exception as e:
        logger.warning("Brave Search failed for '%s': %s", name, e)

    return None, ""


def _apply_fallback_data(company, data: dict, source: str) -> None:
    """Apply data from a fallback source to the CompanyRecord."""
    if not company.matched_name:
        company.matched_name = data.get("name")
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

    officers = data.get("officers", [])
    if officers and not company.officers:
        company.officers = json.dumps(officers, ensure_ascii=False)

    # Store raw response
    raw_json = json.dumps(data, ensure_ascii=False)
    if source == "opencorporates":
        company.opencorporates_raw = raw_json
    elif source == "gleif":
        company.gleif_raw = raw_json
    elif source == "pappers":
        company.pappers_raw = raw_json
    elif source == "brave_search":
        company.brave_raw = raw_json

    # Track data sources
    existing = company.data_sources_used or ""
    sources = [s.strip() for s in existing.split(",") if s.strip()]
    if source not in sources:
        sources.append(source)
    company.data_sources_used = ",".join(sources)
