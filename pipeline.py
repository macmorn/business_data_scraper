"""Main pipeline orchestrator for EU company research."""

from __future__ import annotations

import asyncio
import logging
import sys

import db
from models import STAGE_NEW
from utils.logging_setup import setup_logging

logger = logging.getLogger(__name__)


async def run() -> None:
    """Run all pipeline stages in sequence. Resumable on restart."""
    setup_logging()
    logger.info("=" * 60)
    logger.info("EU Company Research Pipeline - Starting")
    logger.info("=" * 60)

    # Initialize database
    db.init_db()

    # Stage 1: PDF extraction (only if DB is empty)
    if db.count_total() == 0:
        from stages.s01_pdf_extract import run as run_extract
        run_extract()
    else:
        logger.info("Database already populated (%d companies), skipping PDF extraction", db.count_total())

    # Log current state
    stats = db.get_stats()
    logger.info("Pipeline state: %s", stats)

    # Stage 2: Northdata lookup
    from stages.s02_northdata import run as run_northdata
    await run_northdata()

    # Stage 3: Registry fallback for companies not found on Northdata
    from stages.s03_registry_fallback import run as run_fallback
    await run_fallback()

    # Stage 4: CEO identification and lookup
    from stages.s04_ceo_lookup import run as run_ceo
    await run_ceo()

    # Stage 5: AI enrichment (disambiguation + career summaries)
    from stages.s05_ai_enrich import run as run_ai
    await run_ai()

    # Stage 6: Normalize fields
    from stages.s06_normalize import run as run_normalize
    run_normalize()

    # Stage 7: Export to CSV
    from stages.s07_export import run as run_export
    run_export()

    # Final summary
    stats = db.get_stats()
    logger.info("=" * 60)
    logger.info("Pipeline complete. Final state: %s", stats)
    logger.info("=" * 60)


def main() -> None:
    """Entry point."""
    asyncio.run(run())


if __name__ == "__main__":
    main()
