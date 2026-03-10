"""Stage 2: Look up companies on Northdata via headless browser."""

from __future__ import annotations

import json
import logging

import config
import db
from clients.northdata_browser import NorthdataClient
from models import (
    STAGE_PENDING_NORTHDATA,
    STAGE_PENDING_FALLBACK,
    STAGE_PENDING_CEO,
    STAGE_PENDING_AI,
)
from utils.logging_setup import ProgressTracker
from utils.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)


async def run() -> None:
    """Process all companies pending Northdata lookup."""
    companies = db.get_pending(STAGE_PENDING_NORTHDATA, limit=10000)
    if not companies:
        logger.info("Stage 2: No companies pending Northdata lookup")
        return

    logger.info("=" * 40)
    logger.info("Stage 2: Northdata Lookup (%d companies)", len(companies))
    logger.info("=" * 40)

    rate_limiter = RateLimiter(config.NORTHDATA_DELAY_SECONDS)
    tracker = ProgressTracker(len(companies), "northdata")
    results = {"found": 0, "multiple": 0, "not_found": 0, "error": 0}

    client = NorthdataClient()
    try:
        await client.start()

        for company in companies:
            await rate_limiter.wait()

            try:
                result = await client.search(company.name_original)
                company.northdata_raw = json.dumps(result, ensure_ascii=False)

                if result["status"] == "found":
                    _apply_company_data(company, result["data"])
                    company.stage = STAGE_PENDING_CEO
                    results["found"] += 1
                    tracker.tick(company.name_original, "found")

                elif result["status"] == "multiple":
                    # Needs AI disambiguation - store matches, advance to AI stage
                    company.stage = STAGE_PENDING_AI
                    company.needs_review_flag = True
                    results["multiple"] += 1
                    tracker.tick(company.name_original, f"multiple ({len(result['matches'])} matches)")

                elif result["status"] == "not_found":
                    company.stage = STAGE_PENDING_FALLBACK
                    results["not_found"] += 1
                    tracker.tick(company.name_original, "not found")

                else:  # error
                    db.mark_failed(company.id, result.get("error", "unknown error"))
                    results["error"] += 1
                    tracker.tick(company.name_original, f"error: {result.get('error', '')}")
                    continue

                db.update_company(company)

            except Exception as e:
                logger.error("Unexpected error for '%s': %s", company.name_original, e)
                db.mark_failed(company.id, str(e))
                results["error"] += 1

    finally:
        await client.stop()

    tracker.summary(results)


def _apply_company_data(company, data: dict) -> None:
    """Apply scraped Northdata data to a CompanyRecord."""
    company.matched_name = data.get("name") or company.name_original
    company.legal_form = data.get("legal_form")
    company.status = data.get("status")
    company.address = data.get("address")
    company.founded_year = data.get("founded_year")
    company.employees_range = data.get("employees_range")
    company.revenue_range = data.get("revenue_range")
    company.last_accounts_year = data.get("last_accounts_year")

    officers = data.get("officers", [])
    if officers:
        company.officers = json.dumps(officers, ensure_ascii=False)

    # Try to extract country from address
    if company.address:
        company.country = _guess_country(company.address)

    # Add data source
    company.data_sources_used = "northdata"


def _guess_country(address: str) -> str | None:
    """Guess country from an address string."""
    addr_lower = address.lower()
    country_hints = {
        "deutschland": "DE", "germany": "DE",
        "österreich": "AT", "austria": "AT",
        "schweiz": "CH", "switzerland": "CH", "suisse": "CH",
        "france": "FR", "frankreich": "FR",
        "nederland": "NL", "netherlands": "NL",
        "belgien": "BE", "belgium": "BE", "belgique": "BE",
        "italia": "IT", "italy": "IT", "italien": "IT",
        "españa": "ES", "spain": "ES", "spanien": "ES",
        "luxembourg": "LU", "luxemburg": "LU",
    }
    for hint, code in country_hints.items():
        if hint in addr_lower:
            return code
    return None
