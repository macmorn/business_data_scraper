"""OpenCorporates API client for company search across EU jurisdictions."""

from __future__ import annotations

import logging

import httpx

import config
from utils.retry import with_retry

logger = logging.getLogger(__name__)

BASE_URL = "https://api.opencorporates.com/v0.4"

# EU jurisdiction codes for OpenCorporates
EU_JURISDICTIONS = [
    "de", "at", "ch", "fr", "nl", "be", "it", "es", "lu", "ie",
    "pt", "dk", "se", "fi", "no", "pl", "cz", "hu", "ro", "bg",
    "hr", "sk", "si", "ee", "lv", "lt", "cy", "mt", "gr",
]


@with_retry(max_attempts=3, base_delay=2.0, exceptions=(httpx.HTTPError, httpx.TimeoutException))
async def search_company(name: str, jurisdiction: str | None = None) -> dict | None:
    """
    Search for a company on OpenCorporates.

    Returns dict with company data or None if not found.
    """
    params = {"q": name}
    if config.OPENCORPORATES_API_KEY:
        params["api_token"] = config.OPENCORPORATES_API_KEY
    if jurisdiction:
        params["jurisdiction_code"] = jurisdiction

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(f"{BASE_URL}/companies/search", params=params)

        if resp.status_code == 429:
            logger.warning("OpenCorporates rate limit hit")
            raise httpx.HTTPError("Rate limited")

        if resp.status_code != 200:
            logger.warning("OpenCorporates returned %d for '%s'", resp.status_code, name)
            return None

        data = resp.json()

    results = data.get("results", {}).get("companies", [])
    if not results:
        return None

    # Take the best match (first result)
    company = results[0].get("company", {})
    return _parse_company(company)


async def search_company_eu(name: str) -> dict | None:
    """Search across EU jurisdictions, returning the best match."""
    # First try without jurisdiction filter
    result = await search_company(name)
    if result:
        # Verify it's an EU jurisdiction
        country = result.get("country", "")
        if country.lower() in EU_JURISDICTIONS:
            return result
    return result


def _parse_company(raw: dict) -> dict:
    """Parse OpenCorporates company response into our standard format."""
    officers = []
    for officer_entry in raw.get("officers", []):
        officer = officer_entry.get("officer", {})
        officers.append({
            "name": officer.get("name", ""),
            "role": officer.get("position", ""),
        })

    return {
        "name": raw.get("name"),
        "country": raw.get("jurisdiction_code", "").upper()[:2],
        "legal_form": raw.get("company_type"),
        "status": _normalize_status(raw.get("current_status")),
        "founded_year": _extract_year(raw.get("incorporation_date")),
        "address": raw.get("registered_address_in_full"),
        "officers": officers,
        "opencorporates_url": raw.get("opencorporates_url"),
        "source": "opencorporates",
    }


def _normalize_status(status: str | None) -> str | None:
    if not status:
        return None
    s = status.lower()
    if any(w in s for w in ["active", "live", "registered", "good standing"]):
        return "active"
    if any(w in s for w in ["dissolved", "closed", "struck off", "liquidat"]):
        return "dissolved"
    return status


def _extract_year(date_str: str | None) -> int | None:
    if not date_str:
        return None
    try:
        return int(date_str[:4])
    except (ValueError, IndexError):
        return None
