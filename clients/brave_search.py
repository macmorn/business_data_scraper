"""Brave Search API client for gap-filling company information."""

from __future__ import annotations

import logging
import re

import httpx

import config
from utils.retry import with_retry

logger = logging.getLogger(__name__)

BASE_URL = "https://api.search.brave.com/res/v1/web/search"


@with_retry(max_attempts=3, base_delay=2.0, exceptions=(httpx.HTTPError, httpx.TimeoutException))
async def search_company(name: str) -> dict | None:
    """
    Search Brave for company information as a last resort.

    Returns dict with whatever data can be extracted from search results.
    """
    if not config.BRAVE_API_KEY:
        logger.debug("Brave API key not configured, skipping")
        return None

    query = f'"{name}" company registry'

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(
            BASE_URL,
            params={"q": query, "count": 10},
            headers={
                "Accept": "application/json",
                "Accept-Encoding": "gzip",
                "X-Subscription-Token": config.BRAVE_API_KEY,
            },
        )

        if resp.status_code == 429:
            logger.warning("Brave rate limit hit")
            raise httpx.HTTPError("Rate limited")
        if resp.status_code != 200:
            logger.warning("Brave returned %d for '%s'", resp.status_code, name)
            return None

        data = resp.json()

    results = data.get("web", {}).get("results", [])
    if not results:
        return None

    return _extract_from_results(name, results)


def _extract_from_results(name: str, results: list[dict]) -> dict:
    """Extract company information from Brave search result snippets."""
    info = {
        "name": name,
        "source": "brave_search",
    }

    all_text = " ".join(
        f"{r.get('title', '')} {r.get('description', '')}" for r in results
    )

    # Try to extract country
    country_patterns = {
        "DE": r"\b(?:Germany|Deutschland|German)\b",
        "AT": r"\b(?:Austria|Österreich|Austrian)\b",
        "CH": r"\b(?:Switzerland|Schweiz|Swiss)\b",
        "FR": r"\b(?:France|French|Français)\b",
        "NL": r"\b(?:Netherlands|Dutch|Nederland)\b",
        "BE": r"\b(?:Belgium|Belgian|Belgique|Belgien)\b",
        "IT": r"\b(?:Italy|Italian|Italia)\b",
        "ES": r"\b(?:Spain|Spanish|España)\b",
        "LU": r"\b(?:Luxembourg|Luxemburg)\b",
    }
    for code, pattern in country_patterns.items():
        if re.search(pattern, all_text, re.I):
            info["country"] = code
            break

    # Try to extract founding year
    year_match = re.search(
        r"(?:founded|established|gegründet|incorporated)\s+(?:in\s+)?(\d{4})",
        all_text, re.I,
    )
    if year_match:
        year = int(year_match.group(1))
        if 1800 <= year <= 2026:
            info["founded_year"] = year

    # Try to extract status
    if re.search(r"\b(?:active|operating|in business)\b", all_text, re.I):
        info["status"] = "active"
    elif re.search(r"\b(?:dissolved|closed|liquidat|bankrupt|insolvent)\b", all_text, re.I):
        info["status"] = "dissolved"

    # Extract address hints
    address_match = re.search(
        r"(?:address|headquarter|HQ|Sitz)[:\s]+([^.]{10,80})",
        all_text, re.I,
    )
    if address_match:
        info["address"] = address_match.group(1).strip()

    return info
