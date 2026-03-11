"""Stage 6: Normalize and standardize all company fields.

Cleans up data consistency: country codes, legal form names,
employee/revenue ranges, and computes final confidence scores.
"""

from __future__ import annotations

import json
import logging
import re

import db
from models import STAGE_PENDING_NORMALIZE, STAGE_PENDING_EXPORT
from utils.logging_setup import ProgressTracker

logger = logging.getLogger(__name__)

# Standardized legal form mapping
LEGAL_FORM_MAP = {
    "gesellschaft mit beschränkter haftung": "GmbH",
    "gesellschaft m.b.h.": "GmbH",
    "aktiengesellschaft": "AG",
    "kommanditgesellschaft": "KG",
    "offene handelsgesellschaft": "OHG",
    "eingetragener kaufmann": "e.K.",
    "unternehmergesellschaft": "UG",
    "societas europaea": "SE",
    "société par actions simplifiée": "SAS",
    "société à responsabilité limitée": "SARL",
    "société anonyme": "SA",
    "besloten vennootschap": "B.V.",
    "naamloze vennootschap": "N.V.",
    "società per azioni": "S.p.A.",
    "società a responsabilità limitata": "S.r.l.",
    "sociedad limitada": "S.L.",
    "sociedad anónima": "S.A.",
    "public limited company": "PLC",
    "limited": "Ltd",
    "private limited": "Ltd",
}

EMPLOYEE_BUCKETS = [
    (1, 10, "1-10"),
    (11, 50, "11-50"),
    (51, 200, "51-200"),
    (201, 500, "201-500"),
    (501, 1000, "501-1000"),
    (1001, float("inf"), "1000+"),
]


def run() -> None:
    """Normalize all pending companies."""
    companies = db.get_pending(STAGE_PENDING_NORMALIZE, limit=10000)
    if not companies:
        logger.info("Stage 6: No companies pending normalization")
        return

    logger.info("=" * 40)
    logger.info("Stage 6: Normalization (%d companies)", len(companies))
    logger.info("=" * 40)

    tracker = ProgressTracker(len(companies), "normalize")

    for company in companies:
        try:
            # Normalize legal form
            if company.legal_form:
                company.legal_form = _normalize_legal_form(company.legal_form)

            # Ensure matched_name has a value
            if not company.matched_name:
                company.matched_name = company.name_original

            # Normalize employee range
            if company.employees_range:
                company.employees_range = _normalize_employee_range(company.employees_range)

            # Compute confidence score if not already set
            if company.confidence_score is None:
                company.confidence_score = _compute_confidence(company)

            # Set needs_review if confidence is low
            if company.confidence_score is not None and company.confidence_score < 0.5:
                company.needs_review_flag = True

            company.stage = STAGE_PENDING_EXPORT
            db.update_company(company)
            tracker.tick(company.name_original, f"confidence={company.confidence_score:.2f}" if company.confidence_score else "normalized")

        except Exception as e:
            logger.error("Normalize error for '%s': %s", company.name_original, e)
            db.mark_failed(company.id, str(e))

    tracker.summary({"normalized": len(companies)})


def _normalize_legal_form(form: str) -> str:
    """Normalize a legal form name to its standard abbreviation."""
    form_lower = form.strip().lower()
    for full, abbrev in LEGAL_FORM_MAP.items():
        if full in form_lower:
            return abbrev
    return form.strip()


def _normalize_employee_range(raw: str) -> str:
    """Normalize employee count to standard range buckets."""
    # Try to extract numbers
    numbers = re.findall(r"[\d,]+", raw.replace(".", ""))
    if not numbers:
        return raw

    try:
        # Take the largest number as the employee count
        count = max(int(n.replace(",", "")) for n in numbers)
        for low, high, label in EMPLOYEE_BUCKETS:
            if low <= count <= high:
                return label
    except ValueError:
        pass

    return raw


def _compute_confidence(company) -> float:
    """Compute an overall confidence score based on data completeness."""
    score = 0.0
    total_weight = 0.0

    # Fields and their weights
    checks = [
        (company.matched_name and company.matched_name != company.name_original, 0.2),
        (company.country is not None, 0.15),
        (company.legal_form is not None, 0.1),
        (company.status is not None, 0.1),
        (company.founded_year is not None, 0.1),
        (company.address is not None, 0.1),
        (company.officers is not None, 0.1),
        (company.ceo_name is not None, 0.1),
        (company.data_sources_used is not None, 0.05),
    ]

    for has_value, weight in checks:
        total_weight += weight
        if has_value:
            score += weight

    return round(score / total_weight, 2) if total_weight > 0 else 0.0
