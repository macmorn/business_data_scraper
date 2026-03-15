"""Stage 1: Extract company records from PDF, deduplicate, and load into queue.

Uses pdftotext (poppler) as the extraction backend because many supplier-list
PDFs use custom font encodings that break Python PDF libraries. The parsing
is driven by layout configs defined in pdf_layouts.py.
"""

from __future__ import annotations

import logging
import os
import re
import subprocess
from pathlib import Path

import config
import db
from models import STAGE_PENDING_NORTHDATA, STAGE_PENDING_FALLBACK
from pdf_layouts import LAYOUTS, DEFAULT_LAYOUT, PDFLayout

logger = logging.getLogger(__name__)


def extract_text_pdftotext(pdf_path: str) -> str:
    """Extract text from PDF using pdftotext (poppler-utils).

    This handles custom font encodings that break pdfplumber/PyMuPDF.
    Requires poppler-utils to be installed (apt-get install poppler-utils).
    """
    path = Path(pdf_path)
    if not path.exists():
        raise FileNotFoundError(f"PDF not found: {pdf_path}")

    result = subprocess.run(
        ["pdftotext", str(path), "-"],
        capture_output=True, text=True, timeout=120,
    )
    if result.returncode != 0:
        raise RuntimeError(f"pdftotext failed: {result.stderr}")

    logger.info("Extracted text from %s (%d characters)", path.name, len(result.stdout))
    return result.stdout


def extract_text_pymupdf(pdf_path: str) -> str:
    """Fallback: extract text using PyMuPDF. Works for standard font PDFs."""
    import fitz
    path = Path(pdf_path)
    if not path.exists():
        raise FileNotFoundError(f"PDF not found: {pdf_path}")

    doc = fitz.open(str(path))
    pages_text = []
    for page in doc:
        pages_text.append(page.get_text())
    text = "\n".join(pages_text)
    logger.info("Extracted text from %s via PyMuPDF (%d characters)", path.name, len(text))
    return text


def extract_text(pdf_path: str) -> str:
    """Extract text from PDF, trying pdftotext first, then PyMuPDF."""
    try:
        return extract_text_pdftotext(pdf_path)
    except (FileNotFoundError, RuntimeError) as e:
        if "PDF not found" in str(e):
            raise
        logger.warning("pdftotext failed (%s), trying PyMuPDF fallback", e)
        return extract_text_pymupdf(pdf_path)


def clean_lines(text: str, layout: PDFLayout) -> list[str]:
    """Strip blank lines and filter out header/footer/boilerplate lines."""
    lines = []
    for raw_line in text.split("\n"):
        line = raw_line.strip()
        if not line:
            continue
        # Skip by substring match
        if any(pat in line for pat in layout.skip_patterns):
            continue
        # Skip exact matches
        if line in layout.skip_exact:
            continue
        # Skip standalone page numbers
        if re.match(r"^\d{1,2}$", line):
            continue
        lines.append(line)
    return lines


def parse_records(lines: list[str], layout: PDFLayout) -> list[dict]:
    """Parse cleaned lines into structured records based on layout config.

    The layout defines a repeating block of N fields. We scan for lines
    matching the key field pattern, then consume the next N-1 lines as
    the remaining fields.
    """
    record_re = re.compile(layout.record_start_pattern)
    fields_per_record = len(layout.fields)
    records = []

    i = 0
    while i <= len(lines) - fields_per_record:
        # Look for the start of a record (the key field)
        key_field = next(f for f in layout.fields if f.is_key)
        if not record_re.match(lines[i]):
            i += 1
            continue

        # Consume all fields in this record block
        record = {}
        valid = True
        for j, field_def in enumerate(layout.fields):
            if i + j >= len(lines):
                valid = False
                break
            record[field_def.name] = lines[i + j]

        if valid:
            records.append(record)
        i += fields_per_record

    logger.info("Parsed %d raw records from PDF", len(records))
    return records


def deduplicate_records(records: list[dict], layout: PDFLayout) -> list[dict]:
    """Deduplicate records by company name, optionally collecting a field."""
    if not layout.has_duplicate_rows:
        return records

    # Determine the name field
    name_field = "company_name"
    for field_def in layout.fields:
        mapped = layout.field_mapping.get(field_def.name, field_def.name)
        if mapped == "name_original":
            name_field = field_def.name
            break

    seen: dict[str, dict] = {}
    unique = []
    collect_field = layout.dedup_collect_field

    for record in records:
        key = record.get(name_field, "").strip().lower()
        if not key:
            continue

        if key not in seen:
            # First occurrence - initialize collected fields
            if collect_field and collect_field in record:
                record["_collected"] = [record[collect_field]]
            seen[key] = record
            unique.append(record)
        else:
            # Duplicate - collect the merge field if configured
            if collect_field and collect_field in record:
                existing = seen[key]
                existing.setdefault("_collected", [])
                val = record[collect_field]
                if val not in existing["_collected"]:
                    existing["_collected"].append(val)

    # Flatten collected fields back
    if collect_field:
        for record in unique:
            collected = record.pop("_collected", [])
            if collected:
                record[collect_field] = "; ".join(collected)

    logger.info("After deduplication: %d unique companies (from %d records)", len(unique), len(records))
    return unique


def apply_field_mapping(records: list[dict], layout: PDFLayout) -> list[dict]:
    """Rename fields from layout-specific names to CompanyRecord names."""
    mapped = []
    for record in records:
        new = {}
        for key, value in record.items():
            mapped_name = layout.field_mapping.get(key, key)
            new[mapped_name] = value
        mapped.append(new)
    return mapped


def build_address(record: dict) -> str | None:
    """Combine address_street, address_city, address_country into one string."""
    parts = []
    for field in ("address_street", "address_city", "address_country"):
        val = record.get(field)
        if val and val.strip():
            parts.append(val.strip())
    return ", ".join(parts) if parts else None


def get_layout() -> PDFLayout:
    """Get the active PDF layout from config."""
    layout_name = os.environ.get("PDF_LAYOUT", DEFAULT_LAYOUT)
    if layout_name not in LAYOUTS:
        logger.warning("Unknown layout '%s', falling back to '%s'", layout_name, DEFAULT_LAYOUT)
        layout_name = DEFAULT_LAYOUT
    layout = LAYOUTS[layout_name]
    logger.info("Using PDF layout: %s (%s)", layout.name, layout.description)
    return layout


def run(country_filter: set[str] | None = None) -> None:
    """Execute PDF extraction stage.

    Args:
        country_filter: If provided, only keep companies from these ISO country
                        codes (e.g. {"DE", "FR"}). If None, uses the default
                        NORTHDATA_COUNTRIES set.
    """
    logger.info("=" * 40)
    logger.info("Stage 1: PDF Extraction")
    logger.info("=" * 40)

    layout = get_layout()
    pdf_path = config.INPUT_PDF

    # Extract text
    text = extract_text(pdf_path)

    # Clean and parse
    lines = clean_lines(text, layout)
    logger.info("Cleaned text: %d non-empty lines", len(lines))

    records = parse_records(lines, layout)
    records = deduplicate_records(records, layout)
    records = apply_field_mapping(records, layout)

    # Load into database
    names = []
    extra_data = {}  # name -> extra fields from PDF
    for record in records:
        name = record.get("name_original", "").strip()
        if not name or len(name) < 2:
            continue
        names.append(name)

        # Store extra data from the PDF (address, country, etc.)
        extras = {}
        address = build_address(record)
        if address:
            extras["address"] = address
        country_raw = record.get("address_country")
        if country_raw:
            extras["country"] = _normalize_country(country_raw)
        for extra_field in ("vendor_code", "cage_code", "product_group"):
            if extra_field in record:
                extras[extra_field] = record[extra_field]
        if extras:
            extra_data[name] = extras

    count = db.load_companies(names)
    logger.info("Loaded %d new companies into queue", count)

    # Apply extra data from PDF to database records
    if extra_data:
        _apply_extra_data(extra_data)

    # Route companies based on country: included → Northdata, excluded → dropped
    kept = _route_by_region(extra_data, country_filter=country_filter)

    logger.info("Stage 1 complete: %d companies queued (of %d extracted)", kept, len(names))


# Countries covered by Northdata (European company search engine)
NORTHDATA_COUNTRIES = {
    "AT", "BE", "BG", "CH", "CZ", "DE", "DK", "EE", "ES", "FI",
    "FR", "GB", "GR", "HR", "HU", "IE", "IT", "LT", "LU", "LV",
    "NL", "NO", "PL", "PT", "RO", "SE", "SI", "SK",
}


def _route_by_region(
    extra_data: dict[str, dict],
    country_filter: set[str] | None = None,
) -> int:
    """Filter companies by country. Non-matching companies are dropped from the pipeline.

    Args:
        extra_data: PDF-sourced extra fields keyed by company name.
        country_filter: Set of ISO country codes to keep. If None, uses
                        NORTHDATA_COUNTRIES as the default filter.
    """
    allowed = country_filter or NORTHDATA_COUNTRIES
    included = []
    excluded = []
    unknown = []

    with db._get_conn() as conn:
        rows = conn.execute(
            "SELECT name_original, country FROM companies WHERE stage = 'new'"
        ).fetchall()

        for row in rows:
            name = row["name_original"]
            country = row["country"]

            if not country:
                unknown.append(name)
            elif country.upper() in allowed:
                included.append(name)
            else:
                excluded.append(name)

        # Advance included + unknown → Northdata
        if included or unknown:
            keep_names = included + unknown
            placeholders = ",".join("?" * len(keep_names))
            conn.execute(
                f"UPDATE companies SET stage = ? WHERE stage = 'new' AND name_original IN ({placeholders})",
                [STAGE_PENDING_NORTHDATA] + keep_names,
            )

        # Drop excluded companies entirely
        if excluded:
            placeholders = ",".join("?" * len(excluded))
            conn.execute(
                f"DELETE FROM companies WHERE stage = 'new' AND name_original IN ({placeholders})",
                excluded,
            )

    logger.info(
        "Filter: %d companies kept (%s), %d dropped, %d unknown country kept",
        len(included), ", ".join(sorted(allowed)), len(excluded), len(unknown),
    )
    if excluded:
        countries = sorted({extra_data.get(n, {}).get("country", "?") for n in excluded})
        logger.info("Dropped countries: %s", ", ".join(countries))
        for name in excluded:
            c = extra_data.get(name, {}).get("country", "?")
            logger.debug("  Dropped [%s] %s", c, name)

    return len(included) + len(unknown)


def _apply_extra_data(extra_data: dict[str, dict]) -> None:
    """Write PDF-sourced extra fields (address, country) into the database."""
    with db._get_conn() as conn:
        for name, extras in extra_data.items():
            updates = []
            params = []
            if "address" in extras:
                updates.append("address = ?")
                params.append(extras["address"])
            if "country" in extras:
                updates.append("country = ?")
                params.append(extras["country"])
            if updates:
                params.append(name)
                conn.execute(
                    f"UPDATE companies SET {', '.join(updates)} WHERE name_original = ?",
                    params,
                )
    logger.info("Applied extra PDF data to %d companies", len(extra_data))


def _normalize_country(country_raw: str) -> str | None:
    """Normalize country name to ISO 3166-1 alpha-2 code."""
    mapping = {
        "germany": "DE", "deutschland": "DE",
        "france": "FR", "frankreich": "FR",
        "spain": "ES", "spanien": "ES", "espana": "ES", "españa": "ES",
        "usa": "US", "united states": "US",
        "great britain": "GB", "united kingdom": "GB", "uk": "GB",
        "italy": "IT", "italien": "IT", "italia": "IT",
        "belgium": "BE", "belgien": "BE", "belgique": "BE",
        "switzerland": "CH", "schweiz": "CH", "suisse": "CH",
        "netherlands": "NL", "nederland": "NL",
        "austria": "AT", "österreich": "AT",
        "luxembourg": "LU", "luxemburg": "LU",
        "portugal": "PT",
        "denmark": "DK", "dänemark": "DK",
        "sweden": "SE", "schweden": "SE",
        "finland": "FI", "finnland": "FI",
        "norway": "NO", "norwegen": "NO",
        "poland": "PL", "polen": "PL",
        "czech republic": "CZ", "tschechien": "CZ",
        "hungary": "HU", "ungarn": "HU",
        "romania": "RO", "rumänien": "RO",
        "ireland": "IE", "irland": "IE",
        "malaysia": "MY",
        "china": "CN",
        "japan": "JP",
        "india": "IN",
        "canada": "CA",
        "mexico": "MX",
        "brazil": "BR",
        "australia": "AU",
        "south korea": "KR",
        "singapore": "SG",
        "taiwan": "TW",
        "turkey": "TR", "türkei": "TR",
        "israel": "IL",
    }
    key = country_raw.strip().lower()
    return mapping.get(key, country_raw.strip()[:2].upper())
