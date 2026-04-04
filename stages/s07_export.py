"""Stage 7: Export completed company data to CSV.

Produces two files:
1. Main CSV with all companies (Google Sheets compatible, UTF-8 BOM)
2. Review CSV with only flagged companies needing manual review

Resumability: Companies are marked 'done' BEFORE writing CSV files.
If the script crashes after marking but before writing, re-running will
find no pending companies and skip export. The CSV can be regenerated
at any time from 'done' companies via db.get_all_for_export().
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

import config
import db
from models import CSV_COLUMNS, STAGE_PENDING_EXPORT, STAGE_DONE

logger = logging.getLogger(__name__)


def run() -> None:
    """Export all completed companies to CSV."""
    companies = db.get_pending(STAGE_PENDING_EXPORT, limit=100000)
    if not companies:
        logger.info("Stage 7: No companies pending export")
        return

    logger.info("=" * 40)
    logger.info("Stage 7: CSV Export (%d companies)", len(companies))
    logger.info("=" * 40)

    # Mark all as done FIRST — prevents duplicates if crash occurs during CSV write.
    # CSV can always be regenerated from 'done' companies.
    for c in companies:
        c.stage = STAGE_DONE
        db.update_company(c)

    # Build dataframe — dynamically map all CSV_COLUMNS from CompanyRecord
    rows = []
    for c in companies:
        row = {}
        for col in CSV_COLUMNS:
            if col == "company_name_original":
                row[col] = c.name_original
            elif col == "matched_name":
                row[col] = c.matched_name or c.name_original
            else:
                row[col] = getattr(c, col, None)
        rows.append(row)

    df = pd.DataFrame(rows, columns=CSV_COLUMNS)

    # Main CSV
    output_path = Path(config.OUTPUT_CSV)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(str(output_path), index=False, encoding="utf-8-sig")
    logger.info("Exported %d companies to %s", len(df), output_path)

    # Review CSV (only flagged companies)
    review_df = df[df["needs_review_flag"] == True]
    if not review_df.empty:
        # Derive review filename from output stem (e.g. "foo_enriched" → "foo_needs_review")
        stem = output_path.stem.replace("_enriched", "")
        review_path = output_path.parent / f"{stem}_needs_review.csv"
        review_df.to_csv(str(review_path), index=False, encoding="utf-8-sig")
        logger.info("Exported %d companies needing review to %s", len(review_df), review_path)

    logger.info("Stage 7 complete: %d companies exported", len(companies))
