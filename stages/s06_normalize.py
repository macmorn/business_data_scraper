"""Stage 6: Normalize and standardize all company fields."""

from __future__ import annotations

import json
import logging
import re

import db
from models import STAGE_PENDING_NORMALIZE, STAGE_PENDING_EXPORT

logger = logging.getLogger(__name__)

# Country code normalization
COUNTRY_ALIASES = {
    "GERMANY": "DE", "DEUTSCHLAND": "DE",
    "AUSTRIA": "AT", "ÖSTERREICH": "AT",
    "SWITZERLAND": "CH", "SCHWEIZ": "CH", "SUISSE": "CH",
    "FRANCE": "FR", "FRANKREICH": "FR",
    "NETHERLANDS": "NL", "NEDERLAND": "NL",
    "BELGIUM": "BE", "BELGIQUE": "BE", "BELGIEN": "BE",
    "ITALY": "IT", "ITALIA": "IT", "ITALIEN": "IT",
    "SPAIN": "ES", "ESPAÑA": "ES", "SPANIEN": "ES",
    "LUXEMBOURG": "LU", "LUXEMBURG": "LU",
    "IRELAND": "IE", "IRLAND": "IE",
    "PORTUGAL": "PT",
    "DENMARK": "DK", "DÄNEMARK": "DK",
    "SWEDEN": "SE", "SCHWEDEN": "SE",
    "FINLAND": "FI", "FINNLAND": "FI",
    "NORWAY": "NO", "NORWEGEN": "NO",
    "POLAND": "PL", "POLEN": "PL",
    "CZECH REPUBLIC": "CZ", "TSCHECHIEN": "CZ",
    "HUNGARY": "HU", "UNGARN": "HU",
    "ROMANIA": "RO", "RUMÄNIEN": "RO",
    "CROATIA": "HR", "KROATIEN": "HR",
}

# Legal form normalization
LEGAL_FORM_MAP = {
    "GESELLSCHAFT MIT BESCHRÄNKTER HAFTUNG": "GmbH",
    "GESELLSCHAFT MIT BESCHRAENKTER HAFTUNG": "GmbH",
    "AKTIENGESELLSCHAFT": "AG",
    "KOMMANDITGESELLSCHAFT": "KG",
    "OFFENE HANDELSGESELLSCHAFT": "OHG",
    "EINGETRAGENER KAUFMANN": "e.K.",
    "UNTERNEHMERGESELLSCHAFT": "UG",
    "SOCIÉTÉ PAR ACTIONS SIMPLIFIÉE": "SAS",
    "SOCIÉTÉ À RESPONSABILITÉ LIMITÉE": "SARL",
    "SOCIÉTÉ ANONYME": "SA",
    "BESLOTEN VENNOOTSCHAP": "BV",
    "NAAMLOZE VENNOOTSCHAP": "NV",
    "SOCIETAS EUROPAEA": "SE",
    "SOCIETÀ PER AZIONI": "S.p.A.",
    "SOCIETÀ A RESPONSABILITÀ LIMITATA": "S.r.l.",
    "SOCIEDAD LIMITADA": "S.L.",
    "SOCIEDAD ANÓNIMA": "S.A.",
    "PUBLIC LIMITED COMPANY": "PLC",
    "LIMITED": "Ltd",
    "PRIVATE LIMITED": "Ltd",
}

EMPLOYEE_BUCKETS = [
    (1, 10, "1-10"),
    (11, 50, "11-50"),
    (51, 200, "51-200"),
    (201, 500, "201-500"),
    (501, 1000, "501-1000"),
    (1001, 5000, "1001-5000"),
    (5001, 10000, "5001-10000"),
    (10001, float("inf"), "10000+"),
]


def run() -> None:
    """Normalize all fields for completed companies."""
    companies = db.get_pending(STAGE_PENDING_NORMALIZE, limit=10000)
    if not companies:
        logger.info("Stage 6: No companies pending normalization")
        return

    logger.info("=" * 40)
    logger.info("Stage 6: Normalize (%d companies)", len(companies))
    logger.info("=" * 40)

    for company in companies:
        try:
            # Normalize country
            if company.country:
                company.country = _normalize_country(company.country)

            # Normalize legal form
            if company.legal_form:
                company.legal_form = _normalize_legal_form(company.legal_form)

            # Normalize employee range
            if company.employees_range:
                company.employees_range = _normalize_employee_range(company.employees_range)

            # Normalize status
            if company.status:
                company.status = _normalize_status(company.status)

            # Compute confidence score
            company.confidence_score = _compute_confidence(company)

            # Set needs_review flag
            if company.confidence_score is not None and company.confidence_score < 0.6:
                company.needs_review_flag = True

            # Ensure matched_name is set
            if not company.matched_name:
                company.matched_name = company.name_original

            company.stage = STAGE_PENDING_EXPORT
            db.update_company(company)

        except Exception as e:
            logger.error("Normalization error for '%s': %s", company.name_original, e)
            db.mark_failed(company.id, str(e))

    logger.info("Stage 6 complete: %d companies normalized", len(companies))


def _normalize_country(country: str) -> str:
    """Normalize country to ISO 3166-1 alpha-2."""
    c = country.strip().upper()
    # Already a 2-letter code
    if len(c) == 2 and c.isalpha():
        return c
    # Look up alias
    return COUNTRY_ALIASES.get(c, c)


def _normalize_legal_form(form: str) -> str:
    """Normalize legal form to standard abbreviation."""
    f = form.strip().upper()
    if f in LEGAL_FORM_MAP:
        return LEGAL_FORM_MAP[f]
    # Check if it's already a standard abbreviation
    for std in LEGAL_FORM_MAP.values():
        if f == std.upper():
            return std
    return form.strip()


def _normalize_employee_range(emp_str: str) -> str:
    """Normalize employee count/range to standard buckets."""
    # Try to extract numbers
    numbers = re.findall(r"\d+", emp_str.replace(",", "").replace(".", ""))
    if not numbers:
        return emp_str

    try:
        if len(numbers) >= 2:
            low, high = int(numbers[0]), int(numbers[-1])
            mid = (low + high) // 2
        else:
            mid = int(numbers[0])

        for lo, hi, label in EMPLOYEE_BUCKETS:
            if lo <= mid <= hi:
                return label
        return emp_str
    except ValueError:
        return emp_str


def _normalize_status(status: str) -> str:
    """Normalize company status."""
    s = status.strip().lower()
    if any(w in s for w in ["active", "aktiv", "registered", "inscrit", "live"]):
        return "active"
    if any(w in s for w in ["dissolved", "gelöscht", "liquidat", "insolv", "closed", "struck"]):
        return "dissolved"
    return status.strip()


def _compute_confidence(company) -> float:
    """Compute overall confidence score (0.0 - 1.0) based on data completeness."""
    score = 0.0
    weights = {
        "matched_name": 0.15,
        "country": 0.15,
        "legal_form": 0.10,
        "status": 0.10,
        "founded_year": 0.05,
        "address": 0.10,
        "officers": 0.10,
        "ceo_name": 0.15,
        "data_sources_used": 0.10,
    }

    for field, weight in weights.items():
        val = getattr(company, field, None)
        if val:
            score += weight

    # Existing confidence from disambiguation overrides if higher
    if company.confidence_score and company.confidence_score > score:
        return company.confidence_score

    return round(score, 2)
