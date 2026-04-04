"""Main pipeline orchestrator for EU company research.

Usage:
    python pipeline.py [OPTIONS]

    # Run with defaults (from .env / config.py)
    python pipeline.py

    # Specify input PDF and layout
    python pipeline.py --input suppliers.pdf --layout airbus_suppliers

    # Filter to specific countries only
    python pipeline.py --countries DE,FR,GB

    # Override output path
    python pipeline.py --output results.csv

    # Fresh run (delete existing DB first)
    python pipeline.py --fresh

    # List available PDF layouts
    python pipeline.py --list-layouts
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

import config
import db
from pdf_layouts import LAYOUTS
from stages.s01_pdf_extract import NORTHDATA_COUNTRIES
from utils.logging_setup import setup_logging
import cache

logger = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="pipeline",
        description="EU Company Research Pipeline — enrich company data from PDF supplier lists",
    )

    parser.add_argument(
        "--input", "-i",
        metavar="FILE",
        help="Path to input file — PDF or Excel (default: from .env INPUT_PDF)",
    )
    parser.add_argument(
        "--output", "-o",
        metavar="CSV",
        help="Path to output CSV file (default: from .env OUTPUT_CSV)",
    )
    parser.add_argument(
        "--layout", "-l",
        metavar="NAME",
        choices=list(LAYOUTS.keys()),
        help=f"PDF parsing layout (choices: {', '.join(LAYOUTS.keys())})",
    )
    parser.add_argument(
        "--countries", "-c",
        metavar="CODES",
        help=(
            "Comma-separated ISO country codes to include (e.g. DE,FR,GB). "
            "Companies from other countries are dropped at ingestion. "
            "Default: all Northdata-covered European countries"
        ),
    )
    parser.add_argument(
        "--fresh",
        action="store_true",
        help="Delete existing database and start from scratch",
    )
    parser.add_argument(
        "--list-layouts",
        action="store_true",
        help="List available PDF layouts and exit",
    )
    parser.add_argument(
        "--db",
        metavar="PATH",
        help="Path to SQLite database (default: from .env DB_PATH)",
    )
    parser.add_argument(
        "--seed-cache",
        metavar="DB_FILE",
        help="Seed the enrichment cache from an existing pipeline database (e.g. data/pipeline.db)",
    )

    return parser.parse_args(argv)


def apply_overrides(args: argparse.Namespace) -> set[str] | None:
    """Apply CLI overrides to config and return country filter set (or None for default)."""
    if args.input:
        config.INPUT_PDF = args.input

    # Auto-derive output and DB paths from input filename (unless explicitly overridden)
    derived_output, derived_db = config.derive_paths(config.INPUT_PDF)
    if args.output:
        config.OUTPUT_CSV = args.output
    else:
        config.OUTPUT_CSV = derived_output
    if args.db:
        config.DB_PATH = args.db
    else:
        config.DB_PATH = derived_db

    if args.layout:
        config.PDF_LAYOUT = args.layout

    country_filter = None
    if args.countries:
        country_filter = {c.strip().upper() for c in args.countries.split(",") if c.strip()}

    return country_filter


async def run(country_filter: set[str] | None = None) -> None:
    """Run all pipeline stages in sequence. Resumable on restart."""
    setup_logging()
    logger.info("=" * 60)
    logger.info("EU Company Research Pipeline - Starting")
    logger.info("=" * 60)

    if country_filter:
        logger.info("Country filter: %s", ", ".join(sorted(country_filter)))

    # Initialize database
    db.init_db()
    cache.init_cache()

    # Stage 1: PDF extraction (only if DB is empty)
    if db.count_total() == 0:
        from stages.s01_pdf_extract import run as run_extract
        run_extract(country_filter=country_filter)
    else:
        logger.info("Database already populated (%d companies), skipping PDF extraction", db.count_total())
        # Recovery: if companies are stuck at 'new' (crash during Stage 1 routing),
        # re-run routing to move them to pending_northdata.
        stuck_at_new = db.count_at_stage("new")
        if stuck_at_new > 0:
            logger.warning(
                "Found %d companies stuck at 'new' stage (likely from interrupted Stage 1). Re-routing...",
                stuck_at_new,
            )
            from stages.s01_pdf_extract import _route_by_region
            _route_by_region({}, country_filter=country_filter)

    # Resolve companies from enrichment cache (skip Stages 2-6 for cache hits)
    from models import STAGE_PENDING_NORTHDATA, STAGE_PENDING_EXPORT
    pending = db.get_pending(STAGE_PENDING_NORTHDATA, limit=100000)
    if pending:
        pending_names = [c.name_original for c in pending]
        cache_hits = cache.lookup_batch(pending_names)
        if cache_hits:
            for company in pending:
                cached = cache_hits.get(company.name_original)
                if cached:
                    for field_name in cache._ENRICHMENT_COLUMNS:
                        if field_name == "name_original":
                            continue
                        cached_val = getattr(cached, field_name, None)
                        if cached_val is not None:
                            setattr(company, field_name, cached_val)
                    company.stage = STAGE_PENDING_EXPORT
                    db.update_company(company)
            logger.info(
                "Cache: %d companies resolved from cache, %d need enrichment",
                len(cache_hits),
                len(pending_names) - len(cache_hits),
            )

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

    # Stage 4b: Corporate structure traversal (follow holding/KG links)
    from stages.s04b_structure import run as run_structure
    await run_structure()

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


def main(argv: list[str] | None = None) -> None:
    """Entry point."""
    args = parse_args(argv)

    # Handle --list-layouts
    if args.list_layouts:
        print("Available PDF layouts:\n")
        for key, layout in LAYOUTS.items():
            fields = ", ".join(f.name for f in layout.fields)
            print(f"  {key}")
            print(f"    {layout.description}")
            print(f"    Fields: {fields}")
            print()
        print(f"Northdata-covered countries ({len(NORTHDATA_COUNTRIES)}):")
        print(f"  {', '.join(sorted(NORTHDATA_COUNTRIES))}")
        return

    # Handle --seed-cache
    if args.seed_cache:
        cache.init_cache()
        count = cache.seed_from_db(args.seed_cache)
        print(f"Seeded {count} records from {args.seed_cache} into enrichment cache")
        return

    # Handle --fresh
    if args.fresh:
        import pathlib
        db_path = args.db or config.DB_PATH
        p = pathlib.Path(db_path)
        if p.exists():
            answer = input(f"This will delete '{p}' and all existing data. Continue? [y/N] ").strip().lower()
            if answer != "y":
                print("Aborted.")
                return
            p.unlink()
            print(f"Deleted {p}")

    country_filter = apply_overrides(args)
    asyncio.run(run(country_filter=country_filter))


if __name__ == "__main__":
    main()
