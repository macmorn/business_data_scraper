"""Stage 6: Normalize and standardize all company fields.

Cleans up data consistency: country codes, legal form names,
employee/revenue ranges, numeric value parsing, and confidence scores.
"""

from __future__ import annotations

import json
import logging
import re

import db
import cache
import config
from pathlib import Path
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

# Multipliers for suffixes in money values
_MULTIPLIERS = {
    "b": 1_000_000_000, "bn": 1_000_000_000, "billion": 1_000_000_000,
    "m": 1_000_000, "mn": 1_000_000, "million": 1_000_000, "mio": 1_000_000,
    "k": 1_000, "thousand": 1_000, "tsd": 1_000,
}


def _parse_money(raw: str | None) -> tuple[int | None, str | None]:
    """Parse a money string into a plain integer and a notes string.

    Returns (numeric_value, notes_string).
    Examples:
        "€1.65B"             → (1650000000, "€1.65B")
        "€12M"               → (12000000, "€12M")
        "€7,600,000"         → (7600000, "€7,600,000")
        "USD 6 million (est)"→ (6000000, "USD 6 million (estimated)")
        None / ""            → (None, None)
    """
    if not raw or not raw.strip():
        return None, None

    original = raw.strip()

    # Detect currency symbol for notes
    currency = ""
    for sym in ("€", "£", "$", "CHF", "USD", "GBP", "EUR"):
        if sym in original.upper() or sym in original:
            currency = sym
            break

    # Remove currency symbols and whitespace around them
    cleaned = re.sub(r"[€£$]", "", original)
    cleaned = re.sub(r"\b(?:EUR|USD|GBP|CHF)\b", "", cleaned, flags=re.I).strip()

    # Remove annotation text like "(estimated)", "via LLM", etc.
    cleaned = re.sub(r"\(.*?\)", "", cleaned).strip()
    cleaned = re.sub(r"\b(?:estimated|approx|approximately|circa|ca)\b\.?", "", cleaned, flags=re.I).strip()

    # Find the numeric part and optional suffix
    match = re.match(
        r"([~≈]?)\s*([\d]+(?:[.,]\d+)?)\s*([a-zA-Z]*)",
        cleaned,
    )
    if not match:
        # Try: "6 million" pattern
        match = re.match(r"([~≈]?)\s*([\d]+(?:[.,]\d+)?)\s+(million|billion|thousand|mio|tsd)", cleaned, re.I)

    if not match:
        return None, original

    num_str = match.group(2)
    suffix = match.group(3).lower().strip() if match.group(3) else ""

    # Before regex-based parsing, handle pure numeric strings with thousands separators.
    # E.g. "7,600,000" or "1,277,538" — these have no suffix letter.
    if not suffix:
        # Check if cleaned string (digits, commas, dots only) is a plain number
        plain = re.sub(r"[~≈\s]", "", cleaned)
        if re.fullmatch(r"[\d,.']+", plain):
            # Determine if commas are thousands separators or decimal
            if "," in plain and "." in plain:
                # "1,234,567.89" → commas are thousands
                plain = plain.replace(",", "")
            elif "," in plain:
                parts = plain.split(",")
                if all(len(p) == 3 for p in parts[1:]):
                    # "7,600,000" → commas are thousands
                    plain = plain.replace(",", "")
                else:
                    # "1,65" → European decimal
                    plain = plain.replace(",", ".")
            # Remove Swiss apostrophe thousands separator
            plain = plain.replace("'", "")
            try:
                value = float(plain)
                return int(round(value)), original
            except ValueError:
                pass

    # Parse the number from regex match
    # Handle decimal separators
    if "," in num_str and "." in num_str:
        num_str = num_str.replace(",", "")
    elif "," in num_str:
        parts = num_str.split(",")
        if len(parts) == 2 and len(parts[1]) == 3:
            num_str = num_str.replace(",", "")
        else:
            num_str = num_str.replace(",", ".")

    try:
        value = float(num_str)
    except ValueError:
        return None, original

    # Apply multiplier
    multiplier = _MULTIPLIERS.get(suffix, 1)
    result = int(round(value * multiplier))

    return result, original


def _parse_employee_count(raw: str | None) -> tuple[int | None, str | None]:
    """Parse an employee count string into a plain integer and notes.

    Returns (numeric_value, notes_string).
    Examples:
        "3,227" → (3227, "3,227")
        "~75"   → (75, "~75")
        "240"   → (240, "240")
    """
    if not raw or not raw.strip():
        return None, None

    original = raw.strip()

    # Remove approximate markers and text
    cleaned = re.sub(r"[~≈]", "", original).strip()
    cleaned = re.sub(r"\b(?:employees|Mitarbeiter|staff|people|approx|approximately|estimated|circa|ca)\b\.?",
                     "", cleaned, flags=re.I).strip()
    cleaned = re.sub(r"\(.*?\)", "", cleaned).strip()

    # Extract number
    numbers = re.findall(r"[\d]+(?:[.,]\d+)?", cleaned)
    if not numbers:
        return None, original

    # Take the first number (usually the count)
    num_str = numbers[0].replace(",", "").replace("'", "")
    try:
        value = int(float(num_str))
        return value, original
    except ValueError:
        return None, original


def _parse_percentage(raw: str | None) -> str | None:
    """Strip % sign from a percentage, keep as string for display."""
    if not raw:
        return raw
    return raw.replace("%", "").strip()


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

            # --- Numeric normalization ---
            source = company.data_sources_used or "unknown"

            # Revenue: parse to integer, store original in notes
            if company.revenue:
                val, notes = _parse_money(company.revenue)
                if val is not None:
                    company.revenue_notes = f"{notes} ({source})"
                    company.revenue = str(val)

            # Employees: parse to integer, store original in notes
            if company.employees_count:
                val, notes = _parse_employee_count(company.employees_count)
                if val is not None:
                    company.employees_notes = f"{notes} ({source})"
                    company.employees_count = str(val)

            # Normalize employee range from the parsed count
            if company.employees_count:
                try:
                    count = int(company.employees_count)
                    for low, high, label in EMPLOYEE_BUCKETS:
                        if low <= count <= high:
                            company.employees_range = label
                            break
                except ValueError:
                    if company.employees_range:
                        company.employees_range = _normalize_employee_range(company.employees_range)

            # Other financial fields: parse to integers
            for field in ("earnings", "total_assets", "equity", "cash_on_hand",
                          "liabilities", "pension_provisions", "cost_of_materials",
                          "wages_and_salaries", "public_funding_total"):
                raw = getattr(company, field, None)
                if raw:
                    val, _ = _parse_money(raw)
                    if val is not None:
                        setattr(company, field, str(val))

            # Equity ratio: strip % sign
            if company.equity_ratio:
                company.equity_ratio = _parse_percentage(company.equity_ratio)

            # Return on sales: strip % sign
            if company.return_on_sales:
                company.return_on_sales = _parse_percentage(company.return_on_sales)

            # Revenue range: keep human-readable (don't parse)
            # employees_range: already normalized above

            # Compute confidence score if not already set
            if company.confidence_score is None:
                company.confidence_score = _compute_confidence(company)

            # Set needs_review if confidence is low
            if company.confidence_score is not None and company.confidence_score < 0.5:
                company.needs_review_flag = True

            company.stage = STAGE_PENDING_EXPORT
            # Write to shared enrichment cache
            source_run = Path(config.INPUT_PDF).stem
            cache.store(company, source_run)
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
    numbers = re.findall(r"[\d,]+", raw.replace(".", ""))
    if not numbers:
        return raw

    try:
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
