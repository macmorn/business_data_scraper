"""Stage 7: Export completed company data to CSV."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

import config
import db
from models import CSV_COLUMNS, STAGE_DONE

logger = logging.getLogger(__name__)


def run() -> None:
    """Export all completed companies to CSV."""
    companies = db.get_all_for_export()
    if not companies:
        logger.info("Stage 7: No companies ready for export")
        return

    logger.info("=" * 40)
    logger.info("Stage 7: CSV Export (%d companies)", len(companies))
    logger.info("=" * 40)

    # Build rows
    rows = []
    for c in companies:
        rows.append({
            "company_name_original": c.name_original,
            "matched_name": c.matched_name or c.name_original,
            "country": c.country or "",
            "legal_form": c.legal_form or "",
            "status": c.status or "unknown",
            "founded_year": c.founded_year or "",
            "address": c.address or "",
            "employees_range": c.employees_range or "",
            "revenue_range": c.revenue_range or "",
            "last_accounts_year": c.last_accounts_year or "",
            "officers": c.officers or "",
            "ceo_name": c.ceo_name or "",
            "ceo_linkedin_url": c.ceo_linkedin_url or "",
            "ceo_current_title": c.ceo_current_title or "",
            "ceo_career_summary": c.ceo_career_summary or "",
            "ceo_confidence": c.ceo_confidence or "not found",
            "data_sources_used": c.data_sources_used or "",
            "confidence_score": c.confidence_score or 0.0,
            "needs_review_flag": c.needs_review_flag,
        })

    df = pd.DataFrame(rows, columns=CSV_COLUMNS)

    # Write main CSV
    output_path = Path(config.OUTPUT_CSV)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    # UTF-8 BOM for Google Sheets compatibility
    df.to_csv(str(output_path), index=False, encoding="utf-8-sig")
    logger.info("Exported %d companies to %s", len(df), output_path)

    # Write needs_review CSV
    review_df = df[df["needs_review_flag"] == True]  # noqa: E712
    if not review_df.empty:
        review_path = output_path.parent / "needs_review.csv"
        review_df.to_csv(str(review_path), index=False, encoding="utf-8-sig")
        logger.info("Exported %d companies needing review to %s", len(review_df), review_path)

    # Mark as done in database
    with db._get_conn() as conn:
        conn.execute(
            "UPDATE companies SET stage = ? WHERE stage = 'pending_export'",
            (STAGE_DONE,),
        )

    # Summary stats
    stats = db.get_stats()
    total = sum(stats.values())
    done = stats.get("done", 0)
    failed = stats.get("failed", 0)
    logger.info(
        "Export complete. Total: %d | Done: %d | Failed: %d | Review needed: %d",
        total, done, failed, len(review_df),
    )
