"""Reset 'done' companies missing AI enrichment back to pending_ai for rerun.

A Claude usage limit during a prior run can leave companies marked 'done' with
empty enrichment (no CEO summary, no structure summary, no claude_web source).
This utility flips those rows back to 'pending_ai' so the next pipeline run
re-enriches them. It does NOT launch the pipeline.

Operates directly on the given SQLite file(s) via bulk SQL, so it can target any
DB regardless of the current .env (cannot reuse db.mark_for_rerun, which is bound
to config.DB_PATH).

Usage:
    uv run python reset_for_rerun.py data/foo.db [data/bar.db ...]
    uv run python reset_for_rerun.py --dry-run data/foo.db
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

# A 'done' row is treated as un-enriched (eligible for rerun) when ANY of:
#   - corporate_structure_summary is empty (Stage 5 Task 5 runs for every
#     company, so its absence is the strongest signal enrichment was cut short)
#   - ceo_career_summary is empty
#   - data_sources_used never picked up the 'claude_web' tag
_MISSING_PREDICATE = """
    stage = 'done' AND (
        corporate_structure_summary IS NULL OR corporate_structure_summary = ''
        OR ceo_career_summary IS NULL OR ceo_career_summary = ''
        OR data_sources_used IS NULL OR data_sources_used NOT LIKE '%claude_web%'
    )
"""


def reset_db(db_path: Path, dry_run: bool) -> tuple[int, int]:
    """Reset eligible rows in one DB. Returns (eligible, total_done)."""
    conn = sqlite3.connect(str(db_path))
    try:
        total_done = conn.execute(
            "SELECT COUNT(*) FROM companies WHERE stage = 'done'"
        ).fetchone()[0]
        eligible = conn.execute(
            f"SELECT COUNT(*) FROM companies WHERE {_MISSING_PREDICATE}"
        ).fetchone()[0]
        if not dry_run and eligible:
            conn.execute(
                f"""UPDATE companies
                    SET stage = 'pending_ai',
                        error = 'reset_for_rerun',
                        updated_at = datetime('now')
                    WHERE {_MISSING_PREDICATE}"""
            )
            conn.commit()
        return eligible, total_done
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("db_paths", nargs="+", type=Path, help="SQLite DB file(s) to reset")
    parser.add_argument(
        "--dry-run", action="store_true", help="Report counts without modifying anything"
    )
    args = parser.parse_args(argv)

    rc = 0
    for db_path in args.db_paths:
        if not db_path.exists():
            print(f"!! {db_path}: not found", file=sys.stderr)
            rc = 1
            continue
        eligible, total_done = reset_db(db_path, args.dry_run)
        verb = "would reset" if args.dry_run else "reset"
        print(f"{db_path}: {verb} {eligible}/{total_done} done rows -> pending_ai")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
