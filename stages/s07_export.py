"""Stage 7: Export completed company data to CSV.

Produces two files:
1. Main CSV with all companies (Google Sheets compatible, UTF-8 BOM)
2. Review CSV with only flagged companies needing manual review
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

    # Build dataframe
    rows = []
    for c in companies:
        rows.append({
            "company_name_original": c.name_original,
            "matched_name": c.matched_name or c.name_original,
            "country": c.country,
            "legal_form": c.legal_form,
            "status": c.status,
            "founded_year": c.founded_year,
            "address": c.address,
            "employees_range": c.employees_range,
            "revenue_range": c.revenue_range,
            "last_accounts_year": c.last_accounts_year,
            "officers": c.officers,
            "ceo_name": c.ceo_name,
            "ceo_linkedin_url": c.ceo_linkedin_url,
            "ceo_current_title": c.ceo_current_title,
            "ceo_career_summary": c.ceo_career_summary,
            "ceo_confidence": c.ceo_confidence,
            "data_sources_used": c.data_sources_used,
            "confidence_score": c.confidence_score,
            "needs_review_flag": c.needs_review_flag,
        })

    df = pd.DataFrame(rows, columns=CSV_COLUMNS)

    # Main CSV
    output_path = Path(config.OUTPUT_CSV)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(str(output_path), index=False, encoding="utf-8-sig")
    logger.info("Exported %d companies to %s", len(df), output_path)

    # Review CSV (only flagged companies)
    review_df = df[df["needs_review_flag"] == True]
    if not review_df.empty:
        review_path = output_path.parent / "needs_review.csv"
        review_df.to_csv(str(review_path), index=False, encoding="utf-8-sig")
        logger.info("Exported %d companies needing review to %s", len(review_df), review_path)

    # Mark all as done
    for c in companies:
        c.stage = STAGE_DONE
        db.update_company(c)

    logger.info("Stage 7 complete: %d companies exported", len(companies))
