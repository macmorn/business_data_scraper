"""Stage 4: Identify CEO from registry data and website scraping."""

from __future__ import annotations

import json
import logging
import re

import db
from clients import website_scraper, claude_ai
from models import STAGE_PENDING_CEO, STAGE_PENDING_AI
from utils.logging_setup import ProgressTracker

logger = logging.getLogger(__name__)

# Roles that indicate CEO/primary leader
CEO_ROLES = [
    "geschäftsführer", "geschaeftsfuehrer", "ceo", "chief executive",
    "managing director", "directeur général", "directeur general",
    "gérant", "gerant", "bestuurder", "amministratore delegato",
    "inhaber", "president", "general manager", "vorstandsvorsitzender",
]


async def run() -> None:
    """Process all companies pending CEO lookup."""
    companies = db.get_pending(STAGE_PENDING_CEO, limit=10000)
    if not companies:
        logger.info("Stage 4: No companies pending CEO lookup")
        return

    logger.info("=" * 40)
    logger.info("Stage 4: CEO Lookup (%d companies)", len(companies))
    logger.info("=" * 40)

    tracker = ProgressTracker(len(companies), "ceo_lookup")
    results = {"from_officers": 0, "from_website": 0, "not_found": 0, "error": 0}

    for company in companies:
        try:
            ceo_found = False

            # Step 1: Try to extract CEO from officers data
            if company.officers:
                ceo = _find_ceo_in_officers(company.officers)
                if ceo:
                    company.ceo_name = ceo["name"]
                    company.ceo_current_title = ceo["role"]
                    company.ceo_confidence = "high"
                    ceo_found = True
                    results["from_officers"] += 1
                    tracker.tick(company.name_original, f"CEO from officers: {ceo['name']}")

            # Step 2: Try website scraping if no CEO found
            if not ceo_found and company.address:
                # Try to find a website URL from the data we have
                website_url = _extract_website_url(company)
                if website_url:
                    ceo = await website_scraper.find_ceo_from_website(
                        company.name_original, website_url
                    )
                    if ceo:
                        company.ceo_name = ceo["name"]
                        company.ceo_current_title = ceo.get("title", "CEO")
                        company.ceo_confidence = "medium"
                        ceo_found = True
                        results["from_website"] += 1
                        tracker.tick(company.name_original, f"CEO from website: {ceo['name']}")

            if not ceo_found:
                company.ceo_confidence = "not found"
                results["not_found"] += 1
                tracker.tick(company.name_original, "CEO not found")

            company.stage = STAGE_PENDING_AI
            db.update_company(company)

        except Exception as e:
            logger.error("Unexpected error for '%s': %s", company.name_original, e)
            db.mark_failed(company.id, str(e))
            results["error"] += 1

    tracker.summary(results)


def _find_ceo_in_officers(officers_json: str) -> dict | None:
    """Find CEO/Geschäftsführer in the officers JSON array."""
    try:
        officers = json.loads(officers_json)
    except (json.JSONDecodeError, TypeError):
        return None

    if not isinstance(officers, list):
        return None

    for officer in officers:
        role = (officer.get("role") or "").lower()
        for ceo_role in CEO_ROLES:
            if ceo_role in role:
                name = officer.get("name", "").strip()
                if name and len(name) > 2:
                    return {"name": name, "role": officer.get("role", "CEO")}

    # If no explicit CEO role found, take the first officer as a fallback
    if officers:
        first = officers[0]
        name = first.get("name", "").strip()
        if name and len(name) > 2:
            return {"name": name, "role": first.get("role", "Officer")}

    return None


def _extract_website_url(company) -> str | None:
    """Try to extract a company website URL from available data."""
    # Check Northdata raw data for website
    if company.northdata_raw:
        try:
            nd = json.loads(company.northdata_raw)
            data = nd.get("data", {})
            if isinstance(data, dict):
                url = data.get("website") or data.get("url")
                if url and "northdata" not in url:
                    return url
        except (json.JSONDecodeError, TypeError):
            pass

    # Try to construct a likely domain from the company name
    name = company.matched_name or company.name_original
    # Remove legal form suffixes and create a simple domain guess
    clean = re.sub(
        r"\s*(GmbH|AG|KG|SE|SA|SAS|SARL|BV|NV|Ltd|Limited|S\.?[A-Z]\.?[A-Z]?\.?|&\s*Co\.?\s*\w+)\s*$",
        "", name, flags=re.IGNORECASE,
    ).strip()
    if clean:
        # Simple domain guess - this is a heuristic, not reliable
        slug = re.sub(r"[^a-zA-Z0-9]", "", clean.lower())
        if len(slug) >= 3:
            return f"https://www.{slug}.com"

    return None
