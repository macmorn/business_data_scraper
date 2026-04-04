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


def extract_text_pdftotext(
    pdf_path: str,
    *,
    use_layout: bool = False,
    skip_pages: int = 0,
) -> str:
    """Extract text from PDF using pdftotext (poppler-utils).

    This handles custom font encodings that break pdfplumber/PyMuPDF.
    Requires poppler-utils to be installed (apt-get install poppler-utils).
    """
    path = Path(pdf_path)
    if not path.exists():
        raise FileNotFoundError(f"PDF not found: {pdf_path}")

    cmd = ["pdftotext"]
    if use_layout:
        cmd.append("-layout")
    if skip_pages > 0:
        cmd.extend(["-f", str(skip_pages + 1)])
    cmd.extend([str(path), "-"])

    result = subprocess.run(
        cmd,
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


def extract_text(
    pdf_path: str,
    *,
    use_layout: bool = False,
    skip_pages: int = 0,
) -> str:
    """Extract text from PDF, trying pdftotext first, then PyMuPDF."""
    try:
        return extract_text_pdftotext(
            pdf_path, use_layout=use_layout, skip_pages=skip_pages,
        )
    except (FileNotFoundError, RuntimeError) as e:
        if "PDF not found" in str(e):
            raise
        logger.warning("pdftotext failed (%s), trying PyMuPDF fallback", e)
        return extract_text_pymupdf(pdf_path)


def clean_lines(text: str, layout: PDFLayout) -> list[str]:
    """Strip blank lines and filter out header/footer/boilerplate lines.

    For tabular layouts (parse_mode="tabular"), leading whitespace is
    preserved since column positions depend on character offsets.
    """
    preserve_indent = layout.parse_mode == "tabular"
    lines = []
    for raw_line in text.split("\n"):
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped:
            continue
        # Skip by substring match
        if any(pat in stripped for pat in layout.skip_patterns):
            continue
        # Skip exact matches
        if stripped in layout.skip_exact:
            continue
        # Skip standalone page numbers
        if re.match(r"^\d{1,2}$", stripped):
            continue
        lines.append(line if preserve_indent else stripped)
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


# Used to detect whether a line is the main header line
_A220_HEADER_DETECT = ["SAP Code", "Vendor Name", "Address"]

# Column display names and their internal field names (in order).
# "Country/Region" is inferred between State and Status.
_A220_COLUMNS: list[tuple[str, str]] = [
    ("SAP Code",           "sap_code"),
    ("Vendor Name",        "vendor_name"),
    ("Address",            "address"),
    ("City",               "city"),
    ("State",              "state"),
    # Country/Region inserted dynamically
    ("Status",             "status"),
    ("Class",              "supplier_class"),
    ("General Limitation", "general_limitation"),
]


def _parse_header_columns(header_line: str) -> list[tuple[str, int]]:
    """Extract (field_name, position) pairs from a header line.

    Inserts country_region between state and status using the midpoint.
    """
    cols: list[tuple[str, int]] = []
    for display, field in _A220_COLUMNS:
        pos = header_line.find(display)
        if pos == -1 and "General" in display:
            pos = header_line.find("General")
        if pos != -1:
            cols.append((field, pos))

    cols.sort(key=lambda x: x[1])

    # Insert country_region between state and status
    state_idx = next((i for i, (n, _) in enumerate(cols) if n == "state"), None)
    status_idx = next((i for i, (n, _) in enumerate(cols) if n == "status"), None)
    if state_idx is not None and status_idx is not None:
        state_pos = cols[state_idx][1]
        status_pos = cols[status_idx][1]
        country_pos = state_pos + (status_pos - state_pos) // 3
        cols.insert(status_idx, ("country_region", country_pos))

    return cols


def _find_text_segments(line: str) -> list[tuple[int, str]]:
    """Find all contiguous text segments in a line, separated by 2+ spaces.

    Returns list of (start_position, text) tuples.
    """
    segments = []
    i = 0
    n = len(line)
    while i < n:
        # Skip whitespace
        while i < n and line[i] == " ":
            i += 1
        if i >= n:
            break
        start = i
        # Consume text (including single spaces within words)
        while i < n:
            if line[i] == " ":
                # Check if this is a multi-space gap (2+)
                j = i
                while j < n and line[j] == " ":
                    j += 1
                if j - i >= 2:
                    break  # column separator
                i = j  # single space, continue
            else:
                i += 1
        segments.append((start, line[start:i].strip()))
    return segments


def _assign_segments_to_columns(
    segments: list[tuple[int, str]],
    col_positions: list[tuple[str, int]],
) -> dict[str, str]:
    """Map text segments to columns by proximity to header positions."""
    record: dict[str, str] = {name: "" for name, _ in col_positions}

    for seg_pos, seg_text in segments:
        # Find the nearest column (by start position)
        best_col = None
        best_dist = float("inf")
        for col_name, col_pos in col_positions:
            dist = abs(seg_pos - col_pos)
            if dist < best_dist:
                best_dist = dist
                best_col = col_name
        if best_col is not None:
            if record[best_col]:
                record[best_col] += " " + seg_text
            else:
                record[best_col] = seg_text

    return record


def parse_tabular_records(
    lines: list[str],
    layout: PDFLayout,
    raw_text_lines: list[str] | None = None,
) -> list[dict]:
    """Parse fixed-width columnar output from pdftotext -layout.

    Uses per-page header recalibration: each time a header line is found,
    column positions are re-extracted. Text segments on each data line are
    mapped to columns by proximity to the header positions.

    Args:
        lines: Cleaned lines (skip_patterns already applied).
        layout: The active PDF layout.
        raw_text_lines: Uncleaned lines from pdftotext (unused, kept for API compat).
    """
    record_re = re.compile(layout.record_start_pattern)
    col_positions: list[tuple[str, int]] | None = None
    records: list[dict] = []
    pre_record_buffer: list[list[tuple[int, str]]] = []

    for line in lines:
        # Detect header line — recalibrate column positions each time
        if all(h in line for h in _A220_HEADER_DETECT):
            col_positions = _parse_header_columns(line)
            pre_record_buffer.clear()
            logger.debug(
                "Header columns: %s",
                [(n, p) for n, p in col_positions],
            )
            continue

        if col_positions is None:
            continue

        # Skip empty lines
        if not line.strip():
            continue

        # Parse text segments from this line
        segments = _find_text_segments(line)
        if not segments:
            continue

        if record_re.match(line):
            # New record — check if the last buffered pre-record line has
            # vendor_name-only text (wrapping above the SAP code line).
            pre_vendor_parts = []
            if pre_record_buffer:
                # Only consider the immediately preceding continuation line(s)
                # that have text ONLY in the vendor_name column
                for buf in pre_record_buffer:
                    buf_assigned = _assign_segments_to_columns(buf, col_positions)
                    vn = buf_assigned.get("vendor_name", "")
                    # Only steal if this line has vendor_name text and
                    # no text in position-sensitive columns (sap, address, city, etc.)
                    other_fields = any(
                        buf_assigned.get(f, "")
                        for f in ("sap_code", "address", "city", "status")
                    )
                    if vn and not other_fields:
                        pre_vendor_parts.append(vn)
            pre_record_buffer.clear()

            record = _assign_segments_to_columns(segments, col_positions)

            if pre_vendor_parts:
                existing_vn = record.get("vendor_name", "")
                full_vn = " ".join(pre_vendor_parts)
                if existing_vn:
                    full_vn += " " + existing_vn
                record["vendor_name"] = full_vn

                # Remove the stolen vendor text from the PREVIOUS record
                if records:
                    prev = records[-1]
                    for part in pre_vendor_parts:
                        pv = prev.get("vendor_name", "")
                        if pv.endswith(" " + part):
                            prev["vendor_name"] = pv[: -(len(part) + 1)]
                        elif pv.endswith(part):
                            prev["vendor_name"] = pv[: -len(part)]

            records.append(record)
        elif records:
            # Continuation line — merge into previous record
            continuation = _assign_segments_to_columns(segments, col_positions)
            prev = records[-1]
            for key, val in continuation.items():
                if val:
                    existing = prev.get(key, "")
                    if existing:
                        prev[key] = existing + " " + val
                    else:
                        prev[key] = val
            # Buffer this line in case it's a pre-record wrap for the NEXT record
            pre_record_buffer.append(segments)
        else:
            # Pre-record continuation (before any SAP code seen on this page)
            pre_record_buffer.append(segments)

    # Post-process: fix multi-word country names split across state/country columns.
    # If "state" doesn't look like a 2-letter state/province code, it's likely the
    # first word of the country name (e.g. "United" from "United Kingdom").
    for record in records:
        state = record.get("state", "").strip()
        country = record.get("country_region", "").strip()
        if state and not re.match(r"^[A-Z]{2}$", state):
            # Merge state into country and clear state
            record["country_region"] = (state + " " + country).strip()
            record["state"] = ""

    logger.info("Parsed %d raw records from tabular PDF", len(records))
    return records


def extract_from_excel(file_path: str, layout: PDFLayout) -> list[dict]:
    """Extract records from an Excel file using openpyxl.

    Maps spreadsheet column headers to internal field names via
    layout.header_mapping.
    """
    import openpyxl

    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Excel file not found: {file_path}")

    wb = openpyxl.load_workbook(str(path), read_only=True, data_only=True)
    ws = wb.active

    rows = list(ws.iter_rows(values_only=True))
    wb.close()

    if not rows:
        logger.warning("Excel file %s is empty", path.name)
        return []

    # First row is headers
    headers = [str(h).strip() if h else "" for h in rows[0]]
    # Map header names to internal field names
    col_map: dict[int, str] = {}
    for i, header in enumerate(headers):
        if header in layout.header_mapping:
            col_map[i] = layout.header_mapping[header]

    if not col_map:
        logger.error("No matching headers found in %s. Headers: %s", path.name, headers)
        return []

    records = []
    for row in rows[1:]:
        record = {}
        for col_idx, field_name in col_map.items():
            val = row[col_idx] if col_idx < len(row) else None
            record[field_name] = str(val).strip() if val is not None else ""
        if any(record.values()):
            records.append(record)

    logger.info("Extracted %d records from Excel %s", len(records), path.name)
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
    layout_name = config.PDF_LAYOUT
    if layout_name not in LAYOUTS:
        logger.warning("Unknown layout '%s', falling back to '%s'", layout_name, DEFAULT_LAYOUT)
        layout_name = DEFAULT_LAYOUT
    layout = LAYOUTS[layout_name]
    logger.info("Using PDF layout: %s (%s)", layout.name, layout.description)
    return layout


def run(country_filter: set[str] | None = None) -> None:
    """Execute data extraction stage (PDF or Excel).

    Args:
        country_filter: If provided, only keep companies from these ISO country
                        codes (e.g. {"DE", "FR"}). If None, uses the default
                        NORTHDATA_COUNTRIES set.
    """
    logger.info("=" * 40)
    logger.info("Stage 1: Data Extraction")
    logger.info("=" * 40)

    layout = get_layout()
    input_path = config.INPUT_PDF

    if layout.parse_mode == "excel" or input_path.endswith((".xlsx", ".xls")):
        # Excel path — no text extraction needed
        records = extract_from_excel(input_path, layout)
        # header_mapping already produces internal field names, so skip
        # apply_field_mapping (it would double-map)
        records = deduplicate_records(records, layout)
    else:
        # PDF path
        text = extract_text(
            input_path,
            use_layout=layout.use_layout_flag,
            skip_pages=layout.skip_pages,
        )
        lines = clean_lines(text, layout)
        logger.info("Cleaned text: %d non-empty lines", len(lines))

        if layout.parse_mode == "tabular":
            raw_lines = text.split("\n")
            records = parse_tabular_records(lines, layout, raw_text_lines=raw_lines)
        else:
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
    # Pre-clean common noise from tabular PDF parsing
    cleaned = country_raw.strip()
    # Strip leading digits/spaces (page numbers merged with country)
    cleaned = re.sub(r"^\d+\s+", "", cleaned)
    # Strip trailing noise like "United", "Country/ Region"
    cleaned = re.sub(r"\s+Country/?.*$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+United$", "", cleaned)
    # Handle reversed word order
    cleaned = re.sub(r"^United\s+", "", cleaned) if "Kingdom" not in cleaned else cleaned
    cleaned = cleaned.strip()
    if not cleaned:
        return None

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
        "united kingdom": "GB", "kingdom united": "GB", "kingdom": "GB",
        "united states": "US", "usa united": "US", "states united": "US",
        "south korea": "KR", "korea south": "KR",
        "republic czech": "CZ",
        "republic of korea": "KR",
        "kong hong": "HK", "hong kong": "HK",
    }
    key = cleaned.lower()
    return mapping.get(key, cleaned[:2].upper())
