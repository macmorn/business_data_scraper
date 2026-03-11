"""
PDF layout configurations for different supplier list formats.

Each layout config describes how to parse a specific PDF format into
company records. The parser uses pdftotext (poppler) as the backend
since many supplier PDFs use custom font encodings that break
Python-based PDF libraries (pdfplumber, PyMuPDF).

To add a new PDF format:
1. Create a new dict following the structure below
2. Add it to LAYOUTS with a descriptive key
3. Set it as the active layout in .env (PDF_LAYOUT=your_key)
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class FieldDef:
    """Defines one field in a repeating record block."""
    name: str           # internal field name (maps to CompanyRecord)
    position: int       # 0-based position within the record block
    is_key: bool = False  # if True, this field identifies start of a new record


@dataclass
class PDFLayout:
    """Configuration for parsing a specific PDF table format."""
    name: str
    description: str

    # Record structure: ordered list of fields in each repeating block
    fields: list[FieldDef]

    # Lines to skip (substrings). Case-sensitive.
    skip_patterns: list[str] = field(default_factory=list)

    # Exact lines to skip
    skip_exact: list[str] = field(default_factory=list)

    # Regex pattern that identifies the start of a new record
    # (applied to the key field)
    record_start_pattern: str = ""

    # Whether the same company can appear multiple times
    # (e.g. once per product group). If True, records are deduplicated.
    has_duplicate_rows: bool = False

    # Field name to collect when deduplicating (e.g. product_group)
    # Multiple values are joined with "; "
    dedup_collect_field: str | None = None

    # Mapping from layout field names to CompanyRecord field names
    # Only needed if they differ. Unmapped fields use their own name.
    field_mapping: dict[str, str] = field(default_factory=dict)


# ============================================================
# Layout: Airbus Approved Suppliers List
# ============================================================
# Repeating 7-line records (each separated by blank lines):
#   vendor_code, company_name, cage_code, street, city, country, product_group
#
# Key issues:
#   - Custom font encoding breaks pdfplumber/PyMuPDF → must use pdftotext
#   - Same company appears once per product group → deduplicate
#   - CAGE code '#' means unknown
#   - Country is pre-parsed in the PDF
# ============================================================

AIRBUS_SUPPLIERS = PDFLayout(
    name="airbus_suppliers",
    description="Airbus Approved Suppliers List (e.g. Nov 2024 format)",
    fields=[
        FieldDef(name="vendor_code", position=0, is_key=True),
        FieldDef(name="company_name", position=1),
        FieldDef(name="cage_code", position=2),
        FieldDef(name="street", position=3),
        FieldDef(name="city", position=4),
        FieldDef(name="country", position=5),
        FieldDef(name="product_group", position=6),
    ],
    record_start_pattern=r"^\d{6}$",  # 6-digit vendor code
    skip_patterns=[
        "Add content here",
        "AIRBUS APPROVAL",
        "Nov ",
        "Disclaimer:",
        "This is the Airbus",
        "Publication of this",
        "contract authorization",
        "Suppliers remain",
        "For any further",
        "AIRBUS AMBER",
        "AIRBUS S.A.S",
        "This document",
        "not be used",
        "AIRBUS, its logo",
        "Airbus Vendor",
        "CAGE Code",
        "Product Group",
        "Airbus property",
        "© AIRBUS",
    ],
    skip_exact=["Street", "City", "Country"],
    has_duplicate_rows=True,
    dedup_collect_field="product_group",
    field_mapping={
        "company_name": "name_original",
        "street": "address_street",
        "city": "address_city",
        "country": "address_country",
    },
)


# ============================================================
# Layout: Simple company name list (one name per line)
# ============================================================
# For PDFs that are just a flat list of company names,
# possibly with numbering or bullet points.
# ============================================================

SIMPLE_NAME_LIST = PDFLayout(
    name="simple_name_list",
    description="Simple list of company names, one per line",
    fields=[
        FieldDef(name="company_name", position=0, is_key=True),
    ],
    record_start_pattern=r".{3,}",  # any line with 3+ chars
    skip_patterns=[],
    skip_exact=[],
    has_duplicate_rows=True,
    dedup_collect_field=None,
    field_mapping={
        "company_name": "name_original",
    },
)


# ============================================================
# Registry of all available layouts
# ============================================================

LAYOUTS: dict[str, PDFLayout] = {
    "airbus_suppliers": AIRBUS_SUPPLIERS,
    "simple_name_list": SIMPLE_NAME_LIST,
}

# Default layout
DEFAULT_LAYOUT = "airbus_suppliers"
