"""Re-scrape Northdata for companies that already have a stored northdata_url.

Why: many companies were found on Northdata but have NO financial data, because
the original scrape ran without a working premium login (the KPI/financials
table is paywalled for anonymous users). This utility re-visits each stored
`northdata_url` with a working login and re-applies the scraped data — pulling
the financials that were previously hidden.

It does NOT search; it re-scrapes the exact URLs already in the DB. It updates
fields IN PLACE and leaves each company's pipeline `stage` unchanged (a 'done'
row stays 'done', now with financials). Freshly-scraped money/employee values
are normalized inline (reusing Stage 6 helpers) so columns stay consistent with
already-normalized rows. Re-exports are left to a normal pipeline run or manual
export.

Login is REQUIRED: if it fails, the script aborts loudly (NorthdataLoginError)
rather than writing empty/anonymous data. Run in a normal terminal (not nested
in a Claude Code session) with working NORTHDATA_EMAIL/PASSWORD in .env:

    uv run python refetch_northdata.py --dry-run "data/foo.db" ["data/bar.db" ...]
    uv run python refetch_northdata.py "data/foo.db"
    uv run python refetch_northdata.py --all-with-url "data/foo.db"   # not just missing-financials

By default only rows that have a northdata_url AND lack financials are re-fetched.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sqlite3
import sys
from pathlib import Path

import config
from clients.northdata_browser import NorthdataClient, NorthdataLoginError
from stages.s02_northdata import apply_company_data
from stages.s06_normalize import _parse_money, _parse_employee_count, EMPLOYEE_BUCKETS
from utils.rate_limiter import RateLimiter

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger("refetch_northdata")

# A row is eligible when it has a northdata_url and (unless --all-with-url) is
# missing all three core financial signals.
_HAS_URL = "northdata_url IS NOT NULL AND northdata_url != ''"
_MISSING_FIN = (
    "(revenue IS NULL OR revenue='') "
    "AND (employees_count IS NULL OR employees_count='') "
    "AND (total_assets IS NULL OR total_assets='')"
)


class _Row:
    """Lightweight mutable carrier so apply_company_data() can set attributes."""

    # Fields apply_company_data() / normalization read or write.
    _FIELDS = (
        "id", "name_original", "matched_name", "country", "address", "legal_form",
        "status", "founded_year", "employees_range", "revenue_range",
        "last_accounts_year", "northdata_url", "register_id", "register_court",
        "lei", "vat_id", "revenue", "earnings", "total_assets", "equity",
        "equity_ratio", "employees_count", "return_on_sales", "cost_of_materials",
        "wages_and_salaries", "cash_on_hand", "liabilities", "pension_provisions",
        "auditor", "financials_json", "public_funding_total", "corporate_purpose",
        "industry_code", "officers", "data_sources_used", "northdata_raw",
        "revenue_notes", "employees_notes",
    )

    def __init__(self, row: sqlite3.Row):
        for f in self._FIELDS:
            setattr(self, f, row[f] if f in row.keys() else None)


def _normalize_financials(rec: _Row) -> None:
    """Apply the same numeric normalization Stage 6 does to the new values."""
    source = rec.data_sources_used or "northdata"
    if rec.revenue:
        val, notes = _parse_money(rec.revenue)
        if val is not None:
            rec.revenue_notes = f"{notes} ({source})"
            rec.revenue = str(val)
    if rec.employees_count:
        val, notes = _parse_employee_count(rec.employees_count)
        if val is not None:
            rec.employees_notes = f"{notes} ({source})"
            rec.employees_count = str(val)
        try:
            count = int(rec.employees_count)
            for low, high, label in EMPLOYEE_BUCKETS:
                if low <= count <= high:
                    rec.employees_range = label
                    break
        except (ValueError, TypeError):
            pass
    for field in ("earnings", "total_assets", "equity", "cash_on_hand",
                  "liabilities", "pension_provisions", "cost_of_materials",
                  "wages_and_salaries", "public_funding_total"):
        raw = getattr(rec, field, None)
        if raw:
            val, _ = _parse_money(raw)
            if val is not None:
                setattr(rec, field, str(val))


_UPDATE_COLS = (
    "matched_name", "legal_form", "status", "address", "founded_year",
    "employees_range", "revenue_range", "last_accounts_year", "northdata_url",
    "register_id", "register_court", "lei", "vat_id", "revenue", "earnings",
    "total_assets", "equity", "equity_ratio", "employees_count", "return_on_sales",
    "cost_of_materials", "wages_and_salaries", "cash_on_hand", "liabilities",
    "pension_provisions", "auditor", "financials_json", "public_funding_total",
    "corporate_purpose", "industry_code", "officers", "data_sources_used",
    "northdata_raw", "revenue_notes", "employees_notes",
)


def _write_back(conn: sqlite3.Connection, rec: _Row) -> None:
    """Persist updated fields in place (stage untouched)."""
    set_clause = ", ".join(f"{c}=?" for c in _UPDATE_COLS) + ", updated_at=datetime('now')"
    values = [getattr(rec, c, None) for c in _UPDATE_COLS] + [rec.id]
    conn.execute(f"UPDATE companies SET {set_clause} WHERE id=?", values)


def _has_financials(rec: _Row) -> bool:
    return bool(
        (rec.revenue or "").strip()
        or (rec.employees_count or "").strip()
        or (rec.total_assets or "").strip()
    )


async def refetch_db(
    client: NorthdataClient,
    rate_limiter: RateLimiter,
    db_path: Path,
    *,
    all_with_url: bool,
    dry_run: bool,
) -> tuple[int, int, int]:
    """Re-fetch eligible rows in one DB. Returns (eligible, updated, gained_financials)."""
    where = _HAS_URL if all_with_url else f"{_HAS_URL} AND {_MISSING_FIN}"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            f"SELECT * FROM companies WHERE {where} ORDER BY id"
        ).fetchall()
        eligible = len(rows)
        logger.info("%s: %d eligible rows (all_with_url=%s)", db_path.name, eligible, all_with_url)
        if dry_run:
            return eligible, 0, 0

        updated = gained = 0
        for row in rows:
            rec = _Row(row)
            url = rec.northdata_url
            await rate_limiter.wait()
            try:
                scraped = await client.scrape_company_url(url)
            except Exception as e:
                logger.warning("Scrape failed for '%s' (%s): %s", rec.name_original, url, e)
                continue

            had_fin = _has_financials(rec)
            apply_company_data(rec, scraped)
            # Tag source + keep a raw copy for traceability.
            src = rec.data_sources_used or ""
            if "northdata" not in src:
                rec.data_sources_used = f"{src},northdata" if src else "northdata"
            try:
                envelope = json.loads(rec.northdata_raw) if rec.northdata_raw else {}
                if not isinstance(envelope, dict):
                    envelope = {}
            except (json.JSONDecodeError, TypeError):
                envelope = {}
            envelope["refetch_scraped_data"] = scraped
            rec.northdata_raw = json.dumps(envelope, ensure_ascii=False)

            _normalize_financials(rec)
            _write_back(conn, rec)
            conn.commit()
            updated += 1
            now_fin = _has_financials(rec)
            if now_fin and not had_fin:
                gained += 1
            logger.info(
                "  %s | revenue=%s | employees=%s | %s",
                rec.name_original[:40], rec.revenue or "?", rec.employees_count or "?",
                "GAINED financials" if (now_fin and not had_fin) else "updated",
            )
        return eligible, updated, gained
    finally:
        conn.close()


async def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("db_paths", nargs="+", type=Path, help="SQLite DB file(s)")
    parser.add_argument("--dry-run", action="store_true", help="Report eligible counts; scrape nothing")
    parser.add_argument(
        "--all-with-url", action="store_true",
        help="Re-fetch every row with a northdata_url, not just those missing financials",
    )
    args = parser.parse_args(argv)

    missing = [p for p in args.db_paths if not p.exists()]
    for p in missing:
        logger.error("DB not found: %s", p)
    if missing:
        return 1

    # Dry-run never needs a browser/login.
    if args.dry_run:
        for db_path in args.db_paths:
            await refetch_db(None, None, db_path, all_with_url=args.all_with_url, dry_run=True)
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
            "Northdata login FAILED (%s) — aborting re-fetch. No data written. "
            "Fix NORTHDATA_EMAIL/PASSWORD (verify the subscription is active on "
            "northdata.com) and retry.", e,
        )
        await client.stop()
        return 1

    if not client._logged_in:
        logger.error("Login did not establish a session — aborting to avoid anonymous (paywalled) scrapes.")
        await client.stop()
        return 1

    rate_limiter = RateLimiter(config.NORTHDATA_DELAY_MIN, config.NORTHDATA_DELAY_MAX)
    grand = [0, 0, 0]
    try:
        for db_path in args.db_paths:
            e, u, g = await refetch_db(
                client, rate_limiter, db_path,
                all_with_url=args.all_with_url, dry_run=False,
            )
            grand[0] += e; grand[1] += u; grand[2] += g
    finally:
        await client.stop()

    logger.info(
        "Done. Eligible=%d, updated=%d, gained_financials=%d across %d DB(s).",
        grand[0], grand[1], grand[2], len(args.db_paths),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
