"""SQLite database layer for pipeline state, caching, and resumability."""

from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path

import config
from models import CompanyRecord, STAGE_NEW, STAGE_FAILED

logger = logging.getLogger(__name__)

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS companies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name_original TEXT NOT NULL UNIQUE,
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
    public_funding_total TEXT,
    corporate_purpose TEXT,
    industry_code TEXT,
    northdata_url TEXT,
    ceo_name TEXT,
    ceo_linkedin_url TEXT,
    ceo_current_title TEXT,
    ceo_career_summary TEXT,
    ceo_confidence TEXT,
    data_sources_used TEXT,
    confidence_score REAL,
    needs_review_flag INTEGER DEFAULT 0,
    stage TEXT NOT NULL DEFAULT 'new',
    error TEXT,
    retry_count INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    northdata_raw TEXT,
    opencorporates_raw TEXT,
    gleif_raw TEXT,
    pappers_raw TEXT,
    brave_raw TEXT
);
"""

_CREATE_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_stage ON companies(stage);
CREATE INDEX IF NOT EXISTS idx_stage_retry ON companies(stage, retry_count);
"""


def _get_conn() -> sqlite3.Connection:
    """Get a database connection with WAL mode enabled."""
    db_path = Path(config.DB_PATH)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """Create tables and indexes if they don't exist."""
    with _get_conn() as conn:
        conn.executescript(_CREATE_TABLE + _CREATE_INDEXES)
    logger.info("Database initialized at %s", config.DB_PATH)


def load_companies(names: list[str]) -> int:
    """Insert deduplicated company names into the queue. Returns count inserted."""
    inserted = 0
    with _get_conn() as conn:
        for name in names:
            name = name.strip()
            if not name:
                continue
            try:
                conn.execute(
                    "INSERT INTO companies (name_original, stage) VALUES (?, ?)",
                    (name, STAGE_NEW),
                )
                inserted += 1
            except sqlite3.IntegrityError:
                pass  # already exists
    logger.info("Loaded %d new companies (skipped %d duplicates)", inserted, len(names) - inserted)
    return inserted


def get_pending(stage: str, limit: int = 100) -> list[CompanyRecord]:
    """Fetch a batch of companies at the given stage, excluding exhausted retries."""
    with _get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM companies WHERE stage = ? AND retry_count < ? ORDER BY id LIMIT ?",
            (stage, config.MAX_RETRIES, limit),
        ).fetchall()
    return [_row_to_record(row) for row in rows]


def count_at_stage(stage: str) -> int:
    """Count companies at a given stage."""
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM companies WHERE stage = ?", (stage,)
        ).fetchone()
    return row[0]


def count_total() -> int:
    """Count total companies in the database."""
    with _get_conn() as conn:
        row = conn.execute("SELECT COUNT(*) FROM companies").fetchone()
    return row[0]


def update_company(record: CompanyRecord) -> None:
    """Update all fields for a company and advance its stage."""
    with _get_conn() as conn:
        conn.execute(
            """UPDATE companies SET
                matched_name=?, country=?, legal_form=?, status=?,
                founded_year=?, address=?, employees_range=?, revenue_range=?,
                last_accounts_year=?, officers=?,
                register_id=?, register_court=?, lei=?, vat_id=?,
                revenue=?, earnings=?, total_assets=?, equity=?,
                equity_ratio=?, employees_count=?, return_on_sales=?,
                cost_of_materials=?, wages_and_salaries=?, cash_on_hand=?,
                liabilities=?, pension_provisions=?, auditor=?,
                financials_json=?, public_funding_total=?,
                corporate_purpose=?, industry_code=?, northdata_url=?,
                ceo_name=?, ceo_linkedin_url=?, ceo_current_title=?,
                ceo_career_summary=?, ceo_confidence=?,
                data_sources_used=?, confidence_score=?, needs_review_flag=?,
                stage=?, error=?, retry_count=?,
                northdata_raw=?, opencorporates_raw=?, gleif_raw=?,
                pappers_raw=?, brave_raw=?,
                updated_at=datetime('now')
            WHERE id=?""",
            (
                record.matched_name, record.country, record.legal_form, record.status,
                record.founded_year, record.address, record.employees_range, record.revenue_range,
                record.last_accounts_year, record.officers,
                record.register_id, record.register_court, record.lei, record.vat_id,
                record.revenue, record.earnings, record.total_assets, record.equity,
                record.equity_ratio, record.employees_count, record.return_on_sales,
                record.cost_of_materials, record.wages_and_salaries, record.cash_on_hand,
                record.liabilities, record.pension_provisions, record.auditor,
                record.financials_json, record.public_funding_total,
                record.corporate_purpose, record.industry_code, record.northdata_url,
                record.ceo_name, record.ceo_linkedin_url, record.ceo_current_title,
                record.ceo_career_summary, record.ceo_confidence,
                record.data_sources_used, record.confidence_score,
                1 if record.needs_review_flag else 0,
                record.stage, record.error, record.retry_count,
                record.northdata_raw, record.opencorporates_raw, record.gleif_raw,
                record.pappers_raw, record.brave_raw,
                record.id,
            ),
        )


def mark_failed(record_id: int, error: str) -> None:
    """Increment retry count and log the error. Move to 'failed' if retries exhausted."""
    with _get_conn() as conn:
        conn.execute(
            """UPDATE companies SET
                error=?, retry_count=retry_count+1,
                stage=CASE WHEN retry_count+1 >= ? THEN ? ELSE stage END,
                updated_at=datetime('now')
            WHERE id=?""",
            (error, config.MAX_RETRIES, STAGE_FAILED, record_id),
        )


def get_stats() -> dict[str, int]:
    """Return counts per stage for progress reporting."""
    with _get_conn() as conn:
        rows = conn.execute(
            "SELECT stage, COUNT(*) as cnt FROM companies GROUP BY stage"
        ).fetchall()
    return {row["stage"]: row["cnt"] for row in rows}


def get_all_for_export() -> list[CompanyRecord]:
    """Fetch all completed companies for CSV export."""
    with _get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM companies WHERE stage IN (?, ?)",
            ("pending_export", "done"),
        ).fetchall()
    return [_row_to_record(row) for row in rows]


def _row_to_record(row: sqlite3.Row) -> CompanyRecord:
    """Convert a database row to a CompanyRecord."""
    return CompanyRecord(
        id=row["id"],
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
        public_funding_total=row["public_funding_total"],
        corporate_purpose=row["corporate_purpose"],
        industry_code=row["industry_code"],
        northdata_url=row["northdata_url"],
        ceo_name=row["ceo_name"],
        ceo_linkedin_url=row["ceo_linkedin_url"],
        ceo_current_title=row["ceo_current_title"],
        ceo_career_summary=row["ceo_career_summary"],
        ceo_confidence=row["ceo_confidence"],
        data_sources_used=row["data_sources_used"],
        confidence_score=row["confidence_score"],
        needs_review_flag=bool(row["needs_review_flag"]),
        stage=row["stage"],
        error=row["error"],
        retry_count=row["retry_count"],
        northdata_raw=row["northdata_raw"],
        opencorporates_raw=row["opencorporates_raw"],
        gleif_raw=row["gleif_raw"],
        pappers_raw=row["pappers_raw"],
        brave_raw=row["brave_raw"],
    )
