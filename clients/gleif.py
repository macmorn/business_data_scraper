"""GLEIF API client for LEI-based company lookup. Free, no auth required."""

from __future__ import annotations

import logging

import httpx

from utils.retry import with_retry

logger = logging.getLogger(__name__)

BASE_URL = "https://api.gleif.org/api/v1"


@with_retry(max_attempts=3, base_delay=1.0, exceptions=(httpx.HTTPError, httpx.TimeoutException))
async def search_company(name: str) -> dict | None:
    """
    Search GLEIF for a company by name.

    Returns dict with company data or None if not found.
    """
    async with httpx.AsyncClient(timeout=30.0) as client:
        # Use fuzzy search endpoint
        resp = await client.get(
            f"{BASE_URL}/fuzzycompletions",
            params={"field": "entity.legalName", "q": name},
        )

        if resp.status_code != 200:
            logger.warning("GLEIF returned %d for '%s'", resp.status_code, name)
            return None

        data = resp.json()

    results = data.get("data", [])
    if not results:
        return None

    # Take the best match
    best = results[0]
    return _parse_lei_record(best)


@with_retry(max_attempts=3, base_delay=1.0, exceptions=(httpx.HTTPError, httpx.TimeoutException))
async def search_company_full(name: str) -> dict | None:
    """Full-text search on LEI records for more comprehensive results."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(
            f"{BASE_URL}/lei-records",
            params={
                "filter[entity.legalName]": name,
                "page[size]": 5,
            },
        )

        if resp.status_code != 200:
            # Try fulltext search as fallback
            resp = await client.get(
                f"{BASE_URL}/lei-records",
                params={
                    "filter[fulltext]": name,
                    "page[size]": 5,
                },
            )
            if resp.status_code != 200:
                return None

        data = resp.json()

    records = data.get("data", [])
    if not records:
        return None

    return _parse_lei_record(records[0])


def _parse_lei_record(record: dict) -> dict:
    """Parse a GLEIF LEI record into our standard format."""
    attrs = record.get("attributes", {})
    entity = attrs.get("entity", {})
    legal_name = entity.get("legalName", {})
    legal_address = entity.get("legalAddress", {})

    # Build address string
    address_parts = []
    for field in ["addressLines", "city", "region", "postalCode", "country"]:
        val = legal_address.get(field)
        if val:
            if isinstance(val, list):
                address_parts.extend(val)
            else:
                address_parts.append(str(val))

    status_raw = entity.get("status", "")
    if status_raw == "ACTIVE":
        status = "active"
    elif status_raw in ("INACTIVE", "DISSOLVED"):
        status = "dissolved"
    else:
        status = status_raw.lower() if status_raw else None

    return {
        "name": legal_name.get("name"),
        "country": legal_address.get("country"),
        "legal_form": entity.get("legalForm", {}).get("id"),
        "status": status,
        "address": ", ".join(address_parts) if address_parts else None,
        "lei": attrs.get("lei"),
        "source": "gleif",
    }
