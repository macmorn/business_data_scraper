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

    # Parse mode: "block" (repeating N-line blocks), "tabular" (fixed-width
    # columns from pdftotext -layout), or "excel" (spreadsheet via openpyxl)
    parse_mode: str = "block"

    # Pass -layout flag to pdftotext for fixed-width column preservation
    use_layout_flag: bool = False

    # Number of leading PDF pages to skip (e.g. disclaimer/cover pages)
    skip_pages: int = 0

    # For Excel parse_mode: mapping from spreadsheet header → internal field name
    header_mapping: dict[str, str] = field(default_factory=dict)


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
# Layout: A220 Program Approved Supplier List (tabular PDF)
# ============================================================
# Fixed-width columnar table extracted with pdftotext -layout.
# Columns: SAP Code, Vendor Name, Address, City, State,
#           Country/Region, Status, Class, General Limitation/Remarks
# The _1.pdf variant adds: Specification Controlled, Description, Limitation
# and has duplicate rows per SAP Code (one per specification).
#
# Key issues:
#   - First 2 pages are disclaimer/cover → skip_pages=2
#   - Multi-line wrapping for long vendor names and class values
#   - Column boundaries parsed dynamically from header line
# ============================================================

A220_SUPPLIERS = PDFLayout(
    name="a220_suppliers",
    description="A220 Program Approved Supplier List (Airbus Canada, tabular format)",
    parse_mode="tabular",
    use_layout_flag=True,
    skip_pages=2,
    fields=[
        FieldDef(name="sap_code", position=0, is_key=True),
        FieldDef(name="vendor_name", position=1),
        FieldDef(name="address", position=2),
        FieldDef(name="city", position=3),
        FieldDef(name="state", position=4),
        FieldDef(name="country_region", position=5),
        FieldDef(name="status", position=6),
        FieldDef(name="supplier_class", position=7),
        FieldDef(name="general_limitation", position=8),
    ],
    record_start_pattern=r"^\s*\d{4,6}\s+",
    skip_patterns=[
        "Airbus Canada Limited",
        "A220 Approved Suppliers List",
        "AIRBUS CANADA APPROVED",
        "QMSF-09-02-04",
        "Airbus Canada Property",
    ],
    skip_exact=[],
    has_duplicate_rows=True,
    dedup_collect_field="description",
    field_mapping={
        "vendor_name": "name_original",
        "address": "address_street",
        "city": "address_city",
        "state": "address_state",
        "country_region": "address_country",
        "sap_code": "vendor_code",
        "supplier_class": "product_group",
    },
)


# ============================================================
# Layout: Boeing Suppliers (Excel, 3 columns)
# ============================================================
# Simple spreadsheet: Company, Country, Category
# No addresses, no identifier codes.
# ============================================================

BOEING_SUPPLIERS = PDFLayout(
    name="boeing_suppliers",
    description="Boeing Suppliers list (Excel, 3 columns: Company, Country, Category)",
    parse_mode="excel",
    fields=[],
    header_mapping={
        "Company": "name_original",
        "Country": "address_country",
        "Category": "product_group",
    },
    field_mapping={},
)


# ============================================================
# Registry of all available layouts
# ============================================================

LAYOUTS: dict[str, PDFLayout] = {
    "airbus_suppliers": AIRBUS_SUPPLIERS,
    "simple_name_list": SIMPLE_NAME_LIST,
    "a220_suppliers": A220_SUPPLIERS,
    "boeing_suppliers": BOEING_SUPPLIERS,
}

# Default layout
DEFAULT_LAYOUT = "airbus_suppliers"
