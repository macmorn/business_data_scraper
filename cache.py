"""Shared enrichment cache across pipeline runs.

Stores fully enriched company records in data/enrichment_cache.db.
Per-file pipeline runs consult the cache before calling external APIs.
Cache hits skip enrichment (Stages 2-6) and go straight to export.
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

from models import CompanyRecord

logger = logging.getLogger(__name__)

CACHE_DB_PATH = "data/enrichment_cache.db"

_ENRICHMENT_COLUMNS = [
    "name_original", "matched_name", "country", "legal_form", "status",
    "founded_year", "address", "employees_range", "revenue_range",
    "last_accounts_year", "officers",
    "register_id", "register_court", "lei", "vat_id",
    "revenue", "earnings", "total_assets", "equity", "equity_ratio",
    "employees_count", "return_on_sales", "cost_of_materials",
    "wages_and_salaries", "cash_on_hand", "liabilities",
    "pension_provisions", "auditor", "financials_json",
    "revenue_notes", "employees_notes", "public_funding_total",
    "corporate_purpose", "industry_code", "northdata_url",
    "ceo_name", "ceo_linkedin_url", "ceo_current_title",
    "ceo_career_summary", "ceo_confidence",
    "corporate_structure_summary",
    "data_sources_used", "confidence_score",
    "northdata_raw", "opencorporates_raw", "gleif_raw",
    "pappers_raw", "brave_raw",
]

_CREATE_CACHE_TABLE = """
CREATE TABLE IF NOT EXISTS enriched_companies (
    name_original TEXT PRIMARY KEY,
    matched_name TEXT,
    country TEXT,
    legal_form TEXT,
    status TEXT,
    founded_year INTEGER,
    address TEXT,
    employees_range TEXT,
    revenue_range TEXT,
    last_accounts_year INTEGER,
    officers TEXT,
    register_id TEXT,
    register_court TEXT,
    lei TEXT,
    vat_id TEXT,
    revenue TEXT,
    earnings TEXT,
    total_assets TEXT,
    equity TEXT,
    equity_ratio TEXT,
    employees_count TEXT,
    return_on_sales TEXT,
    cost_of_materials TEXT,
    wages_and_salaries TEXT,
    cash_on_hand TEXT,
    liabilities TEXT,
    pension_provisions TEXT,
    auditor TEXT,
    financials_json TEXT,
    revenue_notes TEXT,
    employees_notes TEXT,
    public_funding_total TEXT,
    corporate_purpose TEXT,
    industry_code TEXT,
    northdata_url TEXT,
    ceo_name TEXT,
    ceo_linkedin_url TEXT,
    ceo_current_title TEXT,
    ceo_career_summary TEXT,
    ceo_confidence TEXT,
    corporate_structure_summary TEXT,
    data_sources_used TEXT,
    confidence_score REAL,
    northdata_raw TEXT,
    opencorporates_raw TEXT,
    gleif_raw TEXT,
    pappers_raw TEXT,
    brave_raw TEXT,
    cached_at TEXT DEFAULT (datetime('now')),
    source_run TEXT
);
"""


def _get_cache_conn() -> sqlite3.Connection:
    """Get a connection to the enrichment cache database."""
    db_path = Path(CACHE_DB_PATH)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.row_factory = sqlite3.Row
    return conn


def init_cache() -> None:
    """Create the enrichment cache table if it doesn't exist."""
    with _get_cache_conn() as conn:
        conn.executescript(_CREATE_CACHE_TABLE)
    logger.info("Enrichment cache initialized at %s", CACHE_DB_PATH)


def lookup(name: str) -> CompanyRecord | None:
    """Look up a company by exact name_original match."""
    with _get_cache_conn() as conn:
        row = conn.execute(
            "SELECT * FROM enriched_companies WHERE name_original = ?",
            (name,),
        ).fetchone()
    if row is None:
        return None
    return _cache_row_to_record(row)


def lookup_batch(names: list[str]) -> dict[str, CompanyRecord]:
    """Bulk lookup by exact name_original match.

    Returns {name_original: CompanyRecord} for all cache hits.
    """
    if not names:
        return {}

    results: dict[str, CompanyRecord] = {}
    with _get_cache_conn() as conn:
        chunk_size = 900
        for i in range(0, len(names), chunk_size):
            chunk = names[i : i + chunk_size]
            placeholders = ",".join("?" * len(chunk))
            rows = conn.execute(
                f"SELECT * FROM enriched_companies WHERE name_original IN ({placeholders})",
                chunk,
            ).fetchall()
            for row in rows:
                record = _cache_row_to_record(row)
                results[record.name_original] = record

    logger.info("Cache lookup: %d/%d hits", len(results), len(names))
    return results


def _cache_row_to_record(row: sqlite3.Row) -> CompanyRecord:
    """Convert a cache row to a CompanyRecord (no stage/id/error fields)."""
    return CompanyRecord(
        name_original=row["name_original"],
        matched_name=row["matched_name"],
        country=row["country"],
        legal_form=row["legal_form"],
        status=row["status"],
        founded_year=row["founded_year"],
        address=row["address"],
        employees_range=row["employees_range"],
        revenue_range=row["revenue_range"],
        last_accounts_year=row["last_accounts_year"],
        officers=row["officers"],
        register_id=row["register_id"],
        register_court=row["register_court"],
        lei=row["lei"],
        vat_id=row["vat_id"],
        revenue=row["revenue"],
        earnings=row["earnings"],
        total_assets=row["total_assets"],
        equity=row["equity"],
        equity_ratio=row["equity_ratio"],
        employees_count=row["employees_count"],
        return_on_sales=row["return_on_sales"],
        cost_of_materials=row["cost_of_materials"],
        wages_and_salaries=row["wages_and_salaries"],
        cash_on_hand=row["cash_on_hand"],
        liabilities=row["liabilities"],
        pension_provisions=row["pension_provisions"],
        auditor=row["auditor"],
        financials_json=row["financials_json"],
        revenue_notes=row["revenue_notes"],
        employees_notes=row["employees_notes"],
        public_funding_total=row["public_funding_total"],
        corporate_purpose=row["corporate_purpose"],
        industry_code=row["industry_code"],
        northdata_url=row["northdata_url"],
        ceo_name=row["ceo_name"],
        ceo_linkedin_url=row["ceo_linkedin_url"],
        ceo_current_title=row["ceo_current_title"],
        ceo_career_summary=row["ceo_career_summary"],
        ceo_confidence=row["ceo_confidence"],
        corporate_structure_summary=row["corporate_structure_summary"],
        data_sources_used=row["data_sources_used"],
        confidence_score=row["confidence_score"],
        northdata_raw=row["northdata_raw"],
        opencorporates_raw=row["opencorporates_raw"],
        gleif_raw=row["gleif_raw"],
        pappers_raw=row["pappers_raw"],
        brave_raw=row["brave_raw"],
    )


def store(record: CompanyRecord, source_run: str) -> None:
    """Upsert a fully enriched company record into the cache."""
    _store_records([record], source_run)


def store_batch(records: list[CompanyRecord], source_run: str) -> None:
    """Bulk upsert enriched company records into the cache."""
    _store_records(records, source_run)
    logger.info("Cached %d enriched records (source: %s)", len(records), source_run)


def _store_records(records: list[CompanyRecord], source_run: str) -> None:
    """Internal: upsert records into the cache table."""
    cols = _ENRICHMENT_COLUMNS + ["source_run"]
    placeholders = ",".join("?" * len(cols))
    col_names = ",".join(cols)
    update_cols = [c for c in _ENRICHMENT_COLUMNS if c != "name_original"]
    update_clause = ",".join(f"{c}=excluded.{c}" for c in update_cols)
    update_clause += ",cached_at=datetime('now')"

    sql = f"""INSERT INTO enriched_companies ({col_names})
              VALUES ({placeholders})
              ON CONFLICT(name_original) DO UPDATE SET {update_clause}"""

    with _get_cache_conn() as conn:
        for record in records:
            values = [getattr(record, col, None) for col in _ENRICHMENT_COLUMNS]
            values.append(source_run)
            conn.execute(sql, values)


def seed_from_db(db_path: str) -> int:
    """Seed the cache from an existing per-file pipeline database.

    Reads all records with stage='done' and inserts them into the cache.
    Returns the number of records seeded.
    """
    source_path = Path(db_path)
    if not source_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    source_run = source_path.stem

    conn = sqlite3.connect(str(source_path))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM companies WHERE stage = 'done'"
    ).fetchall()
    conn.close()

    if not rows:
        logger.warning("No completed records found in %s", db_path)
        return 0

    from db import _row_to_record
    records = [_row_to_record(row) for row in rows]

    store_batch(records, source_run)
    logger.info("Seeded %d records from %s into enrichment cache", len(records), db_path)
    return len(records)
