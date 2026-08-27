#!/usr/bin/env python3
"""Export currently enriched companies without running the full pipeline."""

from __future__ import annotations

import argparse
import logging

import config
import db
from models import STAGE_DONE, STAGE_PENDING_EXPORT, STAGE_PENDING_NORMALIZE
from utils.logging_setup import setup_logging

logger = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="export-ready",
        description=(
            "Normalize and export companies that are already enriched, leaving "
            "unfinished pipeline rows parked for later."
        ),
    )
    parser.add_argument(
        "--input",
        "-i",
        metavar="FILE",
        help="Input PDF/Excel path used for the run; derives DB and output paths.",
    )
    parser.add_argument(
        "--db",
        metavar="PATH",
        help="SQLite database path (overrides derived/default DB_PATH).",
    )
    parser.add_argument(
        "--output",
        "-o",
        metavar="CSV",
        help="Output CSV path (also writes the matching .xlsx file).",
    )
    parser.add_argument(
        "--skip-normalize",
        action="store_true",
        help="Export pending_export/done rows only; do not run Stage 6 first.",
    )
    return parser.parse_args(argv)


def apply_overrides(args: argparse.Namespace) -> None:
    if args.input:
        config.INPUT_PDF = args.input
        derived_output, derived_db = config.derive_paths(config.INPUT_PDF)
        config.OUTPUT_CSV = derived_output
        config.DB_PATH = derived_db

    if args.db:
        config.DB_PATH = args.db
    if args.output:
        config.OUTPUT_CSV = args.output


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    apply_overrides(args)
    setup_logging()
    db.init_db()

    stats_before = db.get_stats()
    ready_to_normalize = stats_before.get(STAGE_PENDING_NORMALIZE, 0)
    ready_to_export = stats_before.get(STAGE_PENDING_EXPORT, 0)
    already_done = stats_before.get(STAGE_DONE, 0)

    logger.info("Partial export DB: %s", config.DB_PATH)
    logger.info("Partial export output: %s", config.OUTPUT_CSV)
    logger.info("Current pipeline state: %s", stats_before)

    if not args.skip_normalize and ready_to_normalize:
        from stages.s06_normalize import run as run_normalize

        run_normalize()
    elif args.skip_normalize and ready_to_normalize:
        logger.info(
            "Skipping normalization; %d rows remain at %s",
            ready_to_normalize,
            STAGE_PENDING_NORMALIZE,
        )

    if ready_to_normalize or ready_to_export or already_done:
        from stages.s07_export import run as run_export

        run_export()
    else:
        logger.info("No enriched rows are ready to export yet")

    logger.info("Final pipeline state: %s", db.get_stats())


if __name__ == "__main__":
    main()
