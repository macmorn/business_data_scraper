"""Data models for the EU company research pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field


# Pipeline stage constants
STAGE_NEW = "new"
STAGE_PENDING_NORTHDATA = "pending_northdata"
STAGE_PENDING_FALLBACK = "pending_fallback"
STAGE_PENDING_CEO = "pending_ceo"
STAGE_PENDING_STRUCTURE = "pending_structure"
STAGE_PENDING_AI = "pending_ai"
STAGE_PENDING_NORMALIZE = "pending_normalize"
STAGE_PENDING_EXPORT = "pending_export"
STAGE_DONE = "done"
STAGE_FAILED = "failed"


@dataclass
class CompanyRecord:
    """Represents a single company throughout the pipeline."""

    id: int | None = None
    name_original: str = ""

    # Enriched data
    matched_name: str | None = None
    country: str | None = None
    legal_form: str | None = None
    status: str | None = None
    founded_year: int | None = None
    address: str | None = None
    employees_range: str | None = None
    revenue_range: str | None = None
    last_accounts_year: int | None = None
    officers: str | None = None  # JSON array string

    # Identification
    register_id: str | None = None  # e.g. "HRB 1878"
    register_court: str | None = None  # e.g. "District Court of Neuss"
    lei: str | None = None  # Legal Entity Identifier
    vat_id: str | None = None  # VAT identification number

    # Financials (most recent year from Northdata KPI table, JSON for history)
    revenue: str | None = None  # Most recent revenue value
    earnings: str | None = None  # Most recent earnings value
    total_assets: str | None = None
    equity: str | None = None
    equity_ratio: str | None = None
    employees_count: str | None = None  # Exact count from KPI table
    return_on_sales: str | None = None
    cost_of_materials: str | None = None
    wages_and_salaries: str | None = None
    cash_on_hand: str | None = None
    liabilities: str | None = None
    pension_provisions: str | None = None
    auditor: str | None = None
    financials_json: str | None = None  # Full KPI history as JSON

    # Notes columns (original formatted values + source context)
    revenue_notes: str | None = None
    employees_notes: str | None = None

    # Public funding
    public_funding_total: str | None = None

    # Corporate purpose / industry
    corporate_purpose: str | None = None
    industry_code: str | None = None  # e.g. "20.5"

    # CEO / leadership
    ceo_name: str | None = None
    ceo_linkedin_url: str | None = None
    ceo_current_title: str | None = None
    ceo_career_summary: str | None = None
    ceo_confidence: str | None = None  # high / medium / not found

    # Corporate structure
    corporate_structure_summary: str | None = None

    # Metadata
    data_sources_used: str | None = None  # comma-separated
    confidence_score: float | None = None  # 0.0 - 1.0
    needs_review_flag: bool = False
    northdata_url: str | None = None  # Direct link to Northdata page

    # Pipeline control
    stage: str = STAGE_NEW
    error: str | None = None
    retry_count: int = 0

    # Raw cached responses (not exported to CSV)
    northdata_raw: str | None = None
    opencorporates_raw: str | None = None
    gleif_raw: str | None = None
    pappers_raw: str | None = None
    brave_raw: str | None = None


# Output CSV column order
CSV_COLUMNS = [
    "company_name_original",
    "matched_name",
    "country",
    "legal_form",
    "status",
    "address",
    "register_id",
    "register_court",
    "lei",
    "vat_id",
    "industry_code",
    "corporate_purpose",
    "founded_year",
    "employees_count",
    "employees_notes",
    "employees_range",
    "revenue",
    "revenue_notes",
    "revenue_range",
    "earnings",
    "total_assets",
    "equity",
    "equity_ratio",
    "return_on_sales",
    "cost_of_materials",
    "wages_and_salaries",
    "cash_on_hand",
    "liabilities",
    "pension_provisions",
    "auditor",
    "public_funding_total",
    "last_accounts_year",
    "officers",
    "ceo_name",
    "ceo_linkedin_url",
    "ceo_current_title",
    "ceo_career_summary",
    "ceo_confidence",
    "corporate_structure_summary",
    "northdata_url",
    "data_sources_used",
    "confidence_score",
    "needs_review_flag",
    "financials_json",
]
