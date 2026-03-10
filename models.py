"""Data models for the EU company research pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field


# Pipeline stage constants
STAGE_NEW = "new"
STAGE_PENDING_NORTHDATA = "pending_northdata"
STAGE_PENDING_FALLBACK = "pending_fallback"
STAGE_PENDING_CEO = "pending_ceo"
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

    # CEO / leadership
    ceo_name: str | None = None
    ceo_linkedin_url: str | None = None
    ceo_current_title: str | None = None
    ceo_career_summary: str | None = None
    ceo_confidence: str | None = None  # high / medium / not found

    # Metadata
    data_sources_used: str | None = None  # comma-separated
    confidence_score: float | None = None  # 0.0 - 1.0
    needs_review_flag: bool = False

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


# Output CSV column order (matches design doc)
CSV_COLUMNS = [
    "company_name_original",
    "matched_name",
    "country",
    "legal_form",
    "status",
    "founded_year",
    "address",
    "employees_range",
    "revenue_range",
    "last_accounts_year",
    "officers",
    "ceo_name",
    "ceo_linkedin_url",
    "ceo_current_title",
    "ceo_career_summary",
    "ceo_confidence",
    "data_sources_used",
    "confidence_score",
    "needs_review_flag",
]
