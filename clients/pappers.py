"""Pappers.fr API client for French company registry lookup."""

from __future__ import annotations

import logging

import httpx

import config
from utils.retry import with_retry

logger = logging.getLogger(__name__)

BASE_URL = "https://api.pappers.fr/v2"


@with_retry(max_attempts=3, base_delay=2.0, exceptions=(httpx.HTTPError, httpx.TimeoutException))
async def search_company(name: str) -> dict | None:
    """
    Search Pappers for a French company.

    Requires PAPPERS_API_KEY to be set. Returns None if key is missing or no results.
    """
    if not config.PAPPERS_API_KEY:
        logger.debug("Pappers API key not configured, skipping")
        return None

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(
            f"{BASE_URL}/recherche",
            params={
                "q": name,
                "api_token": config.PAPPERS_API_KEY,
                "par_page": 5,
            },
        )

        if resp.status_code == 401:
            logger.warning("Pappers API key is invalid")
            return None
        if resp.status_code == 429:
            logger.warning("Pappers rate limit hit")
            raise httpx.HTTPError("Rate limited")
        if resp.status_code != 200:
            logger.warning("Pappers returned %d for '%s'", resp.status_code, name)
            return None

        data = resp.json()

    results = data.get("resultats", [])
    if not results:
        return None

    return _parse_company(results[0])


def _parse_company(raw: dict) -> dict:
    """Parse Pappers company response into our standard format."""
    # Extract officers
    officers = []
    for rep in raw.get("representants", []):
        if rep.get("qualite"):
            officers.append({
                "name": f"{rep.get('prenom', '')} {rep.get('nom', '')}".strip(),
                "role": rep.get("qualite", ""),
            })

    # Build address
    address_parts = []
    siege = raw.get("siege", {})
    for field in ["adresse_ligne_1", "adresse_ligne_2", "code_postal", "ville"]:
        val = siege.get(field)
        if val:
            address_parts.append(val)

    # Extract founding year from date_creation
    founded_year = None
    date_creation = raw.get("date_creation")
    if date_creation:
        try:
            founded_year = int(date_creation[:4])
        except (ValueError, IndexError):
            pass

    return {
        "name": raw.get("nom_entreprise"),
        "country": "FR",
        "legal_form": raw.get("forme_juridique"),
        "status": "active" if raw.get("statut_rcs") == "Inscrit" else raw.get("statut_rcs"),
        "founded_year": founded_year,
        "address": ", ".join(address_parts) if address_parts else None,
        "officers": officers,
        "employees_range": raw.get("tranche_effectif"),
        "revenue_range": _format_revenue(raw.get("chiffre_affaires")),
        "last_accounts_year": _extract_year(raw.get("derniere_mise_a_jour_bilan")),
        "siren": raw.get("siren"),
        "source": "pappers",
    }


def _format_revenue(amount) -> str | None:
    if amount is None:
        return None
    try:
        val = float(amount)
        if val >= 1_000_000_000:
            return f"{val / 1_000_000_000:.1f} Mrd EUR"
        elif val >= 1_000_000:
            return f"{val / 1_000_000:.1f} Mio EUR"
        elif val >= 1_000:
            return f"{val / 1_000:.0f}k EUR"
        return f"{val:.0f} EUR"
    except (ValueError, TypeError):
        return str(amount)


def _extract_year(date_str) -> int | None:
    if not date_str:
        return None
    try:
        return int(str(date_str)[:4])
    except (ValueError, IndexError):
        return None
