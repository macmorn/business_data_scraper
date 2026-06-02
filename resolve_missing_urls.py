"""Resolve a Northdata URL via Claude for companies that don't have one yet.

Many companies were marked not_found by Northdata's autocomplete and never got a
`northdata_url`. Stage 2 has a Claude fallback (`_resolve_with_claude`) that uses
web search to find the correct name/URL, then scrapes it. This utility runs that
exact fallback over the companies still missing a `northdata_url`, so they can
gain a URL + financials + officers (parser/login bugs are now fixed).

On success the routine applies the scraped data and advances the company to
`pending_ceo` (so it re-flows through CEO/structure/AI on the next pipeline run)
— this is the routine's native behaviour and matches the chosen scope.

Requires BOTH a working Northdata login (for the scrape) AND Claude usage
headroom (the resolution is a web-search call). If login fails it aborts loudly;
if the Claude usage limit is hit it stops and leaves the rest untouched for a
later rerun.

Run in a normal terminal (not nested in a Claude Code session):

    uv run python resolve_missing_urls.py --dry-run "data/foo.db" ["data/bar.db" ...]
    uv run python resolve_missing_urls.py "data/foo.db" "data/bar.db"
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sqlite3
import sys
from pathlib import Path

import config
import db
from clients.northdata_browser import NorthdataClient, NorthdataLoginError
from clients import claude_ai
from stages.s02_northdata import _resolve_with_claude
from utils.rate_limiter import RateLimiter

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger("resolve_missing_urls")

_NO_URL = "(northdata_url IS NULL OR northdata_url = '')"


def _missing_url_ids(db_path: Path) -> list[int]:
    """Return ids of companies in this DB that have no northdata_url (any stage)."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            f"SELECT id FROM companies WHERE {_NO_URL} ORDER BY id"
        ).fetchall()
        return [r["id"] for r in rows]
    finally:
        conn.close()


def _load_record(record_id: int):
    """Load one company as a CompanyRecord from the current config.DB_PATH."""
    conn = db._get_conn()
    try:
        row = conn.execute("SELECT * FROM companies WHERE id = ?", (record_id,)).fetchone()
    finally:
        conn.close()
    return db._row_to_record(row) if row else None


async def resolve_db(
    client: NorthdataClient,
    rate_limiter: RateLimiter,
    db_path: Path,
    *,
    dry_run: bool,
) -> tuple[int, int, bool]:
    """Resolve missing-URL companies in one DB.

    Returns (eligible, resolved, hit_usage_limit).
    """
    # Bind db.* to this file so update_company / _row_to_record target it.
    config.DB_PATH = str(db_path)
    ids = _missing_url_ids(db_path)
    logger.info("%s: %d companies missing a northdata_url", db_path.name, len(ids))
    if dry_run:
        return len(ids), 0, False

    resolved = 0
    for record_id in ids:
        company = _load_record(record_id)
        if company is None:
            continue
        try:
            ok = await _resolve_with_claude(company, client, rate_limiter)
        except claude_ai.ClaudeUsageLimitError as e:
            logger.error(
                "Claude usage limit hit at '%s' (subtype=%s) — stopping. "
                "Remaining missing-URL companies are untouched; re-run later.",
                company.name_original, e.subtype,
            )
            return len(ids), resolved, True
        except Exception as e:
            logger.warning("Resolve error for '%s': %s", company.name_original, e)
            continue

        if ok:
            db.update_company(company)  # routine mutated fields + stage; persist
            resolved += 1
            logger.info(
                "  RESOLVED '%s' → %s | url=%s | revenue=%s",
                company.name_original, company.matched_name,
                company.northdata_url or "?", company.revenue or "?",
            )
        else:
            logger.info("  unresolved '%s' (left as-is)", company.name_original)

    return len(ids), resolved, False


async def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("db_paths", nargs="+", type=Path, help="SQLite DB file(s)")
    parser.add_argument("--dry-run", action="store_true", help="Report eligible counts; resolve nothing")
    args = parser.parse_args(argv)

    missing = [p for p in args.db_paths if not p.exists()]
    for p in missing:
        logger.error("DB not found: %s", p)
    if missing:
        return 1

    if args.dry_run:
        total = 0
        for db_path in args.db_paths:
            e, _, _ = await resolve_db(None, None, db_path, dry_run=True)
            total += e
        logger.info("Dry-run: %d companies missing a northdata_url across %d DB(s).", total, len(args.db_paths))
        return 0

    client = NorthdataClient(
        email=config.NORTHDATA_EMAIL,
        password=config.NORTHDATA_PASSWORD,
        retry_attempts=config.NORTHDATA_RETRY_ATTEMPTS,
    )
    try:
        await client.start()
    except NorthdataLoginError as e:
        logger.error(
            "Northdata login FAILED (%s) — aborting. Resolved URLs must be "
            "scraped, so a working login is required. Fix NORTHDATA_EMAIL/"
            "PASSWORD and retry.", e,
        )
        await client.stop()
        return 1

    if not client._logged_in:
        logger.error("Login did not establish a session — aborting to avoid anonymous (paywalled) scrapes.")
        await client.stop()
        return 1

    rate_limiter = RateLimiter(config.NORTHDATA_DELAY_MIN, config.NORTHDATA_DELAY_MAX)
    grand_eligible = grand_resolved = 0
    stopped = False
    try:
        for db_path in args.db_paths:
            e, r, hit = await resolve_db(client, rate_limiter, db_path, dry_run=False)
            grand_eligible += e
            grand_resolved += r
            if hit:
                stopped = True
                break  # usage limit: stop before touching the next DB
    finally:
        await client.stop()

    logger.info(
        "Done%s. Eligible=%d, resolved=%d.",
        " (stopped at usage limit)" if stopped else "",
        grand_eligible, grand_resolved,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
