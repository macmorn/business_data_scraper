# Enrichment Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a shared enrichment cache so companies enriched in one pipeline run are reusable by subsequent runs without re-calling external APIs.

**Architecture:** A new `cache.py` module manages `data/enrichment_cache.db` — a single SQLite DB with one `enriched_companies` table. After Stage 1 extraction, companies are looked up in the cache; hits skip to export, misses go through normal enrichment. After Stage 6 normalization, newly enriched records are written to the cache. A `--seed-cache` CLI flag migrates existing data from `pipeline.db`.

**Tech Stack:** Python 3, SQLite, existing `models.CompanyRecord` dataclass

---

## File Structure

| File | Action | Responsibility |
| ---- | ------ | -------------- |
| `cache.py` | Create | Cache DB init, lookup, store, seed operations |
| `pipeline.py` | Modify | Add `--seed-cache` CLI, call `cache.init_cache()`, insert cache lookup between Stage 1 and Stage 2 |
| `stages/s06_normalize.py` | Modify | Call `cache.store()` after normalizing each company |

---

### Task 1: Create `cache.py` — Cache Module

**Files:**
- Create: `cache.py`

The cache table mirrors the `companies` table from `db.py` but excludes pipeline control fields (`stage`, `error`, `retry_count`, `needs_review_flag`). It adds `cached_at` and `source_run`.

- [ ] **Step 1: Create `cache.py` with schema and `init_cache()`**

```python
"""Shared enrichment cache across pipeline runs.

Stores fully enriched company records in data/enrichment_cache.db.
Per-file pipeline runs consult the cache before calling external APIs.
Cache hits skip enrichment (Stages 2-6) and go straight to export.
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

from models import CompanyRecord

logger = logging.getLogger(__name__)

CACHE_DB_PATH = "data/enrichment_cache.db"

# All enrichment columns from the companies table, excluding pipeline control
# (stage, error, retry_count, needs_review_flag) and id.
_ENRICHMENT_COLUMNS = [
    "name_original", "matched_name", "country", "legal_form", "status",
    "founded_year", "address", "employees_range", "revenue_range",
    "last_accounts_year", "officers",
    "register_id", "register_court", "lei", "vat_id",
    "revenue", "earnings", "total_assets", "equity", "equity_ratio",
    "employees_count", "return_on_sales", "cost_of_materials",
    "wages_and_salaries", "cash_on_hand", "liabilities",
    "pension_provisions", "auditor", "financials_json",
    "revenue_notes", "employees_notes", "public_funding_total",
    "corporate_purpose", "industry_code", "northdata_url",
    "ceo_name", "ceo_linkedin_url", "ceo_current_title",
    "ceo_career_summary", "ceo_confidence",
    "corporate_structure_summary",
    "data_sources_used", "confidence_score",
    "northdata_raw", "opencorporates_raw", "gleif_raw",
    "pappers_raw", "brave_raw",
]

_CREATE_CACHE_TABLE = """
CREATE TABLE IF NOT EXISTS enriched_companies (
    name_original TEXT PRIMARY KEY,
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
    revenue_notes TEXT,
    employees_notes TEXT,
    public_funding_total TEXT,
    corporate_purpose TEXT,
    industry_code TEXT,
    northdata_url TEXT,
    ceo_name TEXT,
    ceo_linkedin_url TEXT,
    ceo_current_title TEXT,
    ceo_career_summary TEXT,
    ceo_confidence TEXT,
    corporate_structure_summary TEXT,
    data_sources_used TEXT,
    confidence_score REAL,
    northdata_raw TEXT,
    opencorporates_raw TEXT,
    gleif_raw TEXT,
    pappers_raw TEXT,
    brave_raw TEXT,
    cached_at TEXT DEFAULT (datetime('now')),
    source_run TEXT
);
"""


def _get_cache_conn() -> sqlite3.Connection:
    """Get a connection to the enrichment cache database."""
    db_path = Path(CACHE_DB_PATH)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.row_factory = sqlite3.Row
    return conn


def init_cache() -> None:
    """Create the enrichment cache table if it doesn't exist."""
    with _get_cache_conn() as conn:
        conn.executescript(_CREATE_CACHE_TABLE)
    logger.info("Enrichment cache initialized at %s", CACHE_DB_PATH)
```

- [ ] **Step 2: Verify `init_cache()` creates the database**

```bash
cd "/Users/antonkozackov/VSCode Projects/SMG/business_data_scraper"
python3 -c "
import cache
cache.init_cache()
import sqlite3
conn = sqlite3.connect('data/enrichment_cache.db')
tables = conn.execute(\"SELECT name FROM sqlite_master WHERE type='table'\").fetchall()
print('Tables:', [t[0] for t in tables])
cols = conn.execute('PRAGMA table_info(enriched_companies)').fetchall()
print(f'Columns: {len(cols)}')
conn.close()
import os; os.unlink('data/enrichment_cache.db')
print('OK')
"
```

Expected: `Tables: ['enriched_companies']`, `Columns: 49`, `OK`

- [ ] **Step 3: Add `lookup()` and `lookup_batch()`**

Append to `cache.py`:

```python
def lookup(name: str) -> CompanyRecord | None:
    """Look up a company by exact name_original match.

    Returns a populated CompanyRecord (with stage/id unset) or None.
    """
    with _get_cache_conn() as conn:
        row = conn.execute(
            "SELECT * FROM enriched_companies WHERE name_original = ?",
            (name,),
        ).fetchone()
    if row is None:
        return None
    return _cache_row_to_record(row)


def lookup_batch(names: list[str]) -> dict[str, CompanyRecord]:
    """Bulk lookup by exact name_original match.

    Returns {name_original: CompanyRecord} for all cache hits.
    """
    if not names:
        return {}

    results: dict[str, CompanyRecord] = {}
    with _get_cache_conn() as conn:
        # SQLite has a variable limit (~999), so batch in chunks
        chunk_size = 900
        for i in range(0, len(names), chunk_size):
            chunk = names[i : i + chunk_size]
            placeholders = ",".join("?" * len(chunk))
            rows = conn.execute(
                f"SELECT * FROM enriched_companies WHERE name_original IN ({placeholders})",
                chunk,
            ).fetchall()
            for row in rows:
                record = _cache_row_to_record(row)
                results[record.name_original] = record

    logger.info("Cache lookup: %d/%d hits", len(results), len(names))
    return results


def _cache_row_to_record(row: sqlite3.Row) -> CompanyRecord:
    """Convert a cache row to a CompanyRecord (no stage/id/error fields)."""
    return CompanyRecord(
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
        revenue_notes=row["revenue_notes"],
        employees_notes=row["employees_notes"],
        public_funding_total=row["public_funding_total"],
        corporate_purpose=row["corporate_purpose"],
        industry_code=row["industry_code"],
        northdata_url=row["northdata_url"],
        ceo_name=row["ceo_name"],
        ceo_linkedin_url=row["ceo_linkedin_url"],
        ceo_current_title=row["ceo_current_title"],
        ceo_career_summary=row["ceo_career_summary"],
        ceo_confidence=row["ceo_confidence"],
        corporate_structure_summary=row["corporate_structure_summary"],
        data_sources_used=row["data_sources_used"],
        confidence_score=row["confidence_score"],
        northdata_raw=row["northdata_raw"],
        opencorporates_raw=row["opencorporates_raw"],
        gleif_raw=row["gleif_raw"],
        pappers_raw=row["pappers_raw"],
        brave_raw=row["brave_raw"],
    )
```

- [ ] **Step 4: Add `store()` and `store_batch()`**

Append to `cache.py`:

```python
def store(record: CompanyRecord, source_run: str) -> None:
    """Upsert a fully enriched company record into the cache."""
    _store_records([record], source_run)


def store_batch(records: list[CompanyRecord], source_run: str) -> None:
    """Bulk upsert enriched company records into the cache."""
    _store_records(records, source_run)
    logger.info("Cached %d enriched records (source: %s)", len(records), source_run)


def _store_records(records: list[CompanyRecord], source_run: str) -> None:
    """Internal: upsert records into the cache table."""
    cols = _ENRICHMENT_COLUMNS + ["source_run"]
    placeholders = ",".join("?" * len(cols))
    col_names = ",".join(cols)
    # ON CONFLICT: update all columns except name_original and source_run
    update_cols = [c for c in _ENRICHMENT_COLUMNS if c != "name_original"]
    update_clause = ",".join(f"{c}=excluded.{c}" for c in update_cols)
    update_clause += ",cached_at=datetime('now')"

    sql = f"""INSERT INTO enriched_companies ({col_names})
              VALUES ({placeholders})
              ON CONFLICT(name_original) DO UPDATE SET {update_clause}"""

    with _get_cache_conn() as conn:
        for record in records:
            values = [getattr(record, col, None) for col in _ENRICHMENT_COLUMNS]
            values.append(source_run)
            conn.execute(sql, values)
```

- [ ] **Step 5: Add `seed_from_db()`**

Append to `cache.py`:

```python
def seed_from_db(db_path: str) -> int:
    """Seed the cache from an existing per-file pipeline database.

    Reads all records with stage='done' and inserts them into the cache.
    Returns the number of records seeded.
    """
    source_path = Path(db_path)
    if not source_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    source_run = source_path.stem  # e.g. "pipeline" or "A220-Program-Approved-Supplier-List"

    conn = sqlite3.connect(str(source_path))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM companies WHERE stage = 'done'"
    ).fetchall()
    conn.close()

    if not rows:
        logger.warning("No completed records found in %s", db_path)
        return 0

    # Convert rows to CompanyRecords using the same mapper as db.py
    from db import _row_to_record
    records = [_row_to_record(row) for row in rows]

    store_batch(records, source_run)
    logger.info("Seeded %d records from %s into enrichment cache", len(records), db_path)
    return len(records)
```

- [ ] **Step 6: Verify cache operations end-to-end**

```bash
cd "/Users/antonkozackov/VSCode Projects/SMG/business_data_scraper"
python3 -c "
import cache
from models import CompanyRecord

cache.init_cache()

# Store a test record
rec = CompanyRecord(name_original='Test Corp', matched_name='Test Corporation', country='DE', revenue='1000000')
cache.store(rec, 'test_run')

# Lookup single
result = cache.lookup('Test Corp')
assert result is not None
assert result.matched_name == 'Test Corporation'
assert result.country == 'DE'
print('Single lookup: OK')

# Lookup batch
results = cache.lookup_batch(['Test Corp', 'Missing Corp'])
assert len(results) == 1
assert 'Test Corp' in results
assert 'Missing Corp' not in results
print('Batch lookup: OK')

# Lookup miss
miss = cache.lookup('Does Not Exist')
assert miss is None
print('Miss: OK')

# Cleanup
import os; os.unlink('data/enrichment_cache.db')
print('All cache tests passed!')
"
```

Expected: All assertions pass, `All cache tests passed!`

- [ ] **Step 7: Commit**

```bash
git add cache.py
git commit -m "feat: add enrichment cache module with lookup, store, and seed operations"
```

---

### Task 2: Integrate Cache Lookup into Pipeline (between Stage 1 and Stage 2)

**Files:**
- Modify: `pipeline.py:117-149` (the `run()` function)

After Stage 1 extraction and before Stage 2, look up all companies in the cache. For cache hits, copy enriched data into the per-file DB and set their stage to `pending_export`, skipping Stages 2-6.

- [ ] **Step 1: Add cache import and `init_cache()` call**

In `pipeline.py`, add the import at the top (after existing imports around line 34):

```python
import cache
```

In the `run()` function, after `db.init_db()` (line 128), add:

```python
    cache.init_cache()
```

- [ ] **Step 2: Add cache lookup after Stage 1**

In `pipeline.py`, after the Stage 1 block (after line 145, before the `# Log current state` comment on line 148), insert the cache resolution step:

```python
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
                    # Copy all enriched fields from cache into the per-file record
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
```

- [ ] **Step 3: Verify cache integration works**

```bash
cd "/Users/antonkozackov/VSCode Projects/SMG/business_data_scraper"
python3 -c "
import cache, config, db
from models import CompanyRecord, STAGE_PENDING_NORTHDATA, STAGE_PENDING_EXPORT

# Setup: create cache with one known company
cache.init_cache()
rec = CompanyRecord(name_original='3M', matched_name='3M Company', country='US', revenue='35000000000')
cache.store(rec, 'test')

# Setup: create a per-file DB with '3M' at pending_northdata
config.DB_PATH = 'data/test_cache_integration.db'
from pathlib import Path
p = Path(config.DB_PATH)
if p.exists(): p.unlink()
db.init_db()
db.load_companies(['3M', 'Unknown Corp'])

# Simulate routing to pending_northdata
with db._get_conn() as conn:
    conn.execute('UPDATE companies SET stage = ? WHERE stage = ?', (STAGE_PENDING_NORTHDATA, 'new'))

# Run the cache lookup logic
pending = db.get_pending(STAGE_PENDING_NORTHDATA, limit=100)
pending_names = [c.name_original for c in pending]
cache_hits = cache.lookup_batch(pending_names)

for company in pending:
    cached = cache_hits.get(company.name_original)
    if cached:
        for field_name in cache._ENRICHMENT_COLUMNS:
            if field_name == 'name_original':
                continue
            cached_val = getattr(cached, field_name, None)
            if cached_val is not None:
                setattr(company, field_name, cached_val)
        company.stage = STAGE_PENDING_EXPORT
        db.update_company(company)

# Verify
stats = db.get_stats()
print(f'Stats: {stats}')
assert stats.get('pending_export', 0) == 1, f'Expected 1 pending_export, got {stats}'
assert stats.get('pending_northdata', 0) == 1, f'Expected 1 pending_northdata, got {stats}'

# Verify the enriched data was copied
exported = db.get_pending(STAGE_PENDING_EXPORT, limit=10)
assert exported[0].matched_name == '3M Company'
assert exported[0].revenue == '35000000000'
print('Cache integration test passed!')

# Cleanup
import os
p.unlink()
os.unlink('data/enrichment_cache.db')
"
```

Expected: `Cache integration test passed!`

- [ ] **Step 4: Commit**

```bash
git add pipeline.py
git commit -m "feat: integrate enrichment cache lookup between Stage 1 and Stage 2"
```

---

### Task 3: Write Enriched Records to Cache After Stage 6

**Files:**
- Modify: `stages/s06_normalize.py:277-278`

After each company is normalized and before it advances to `pending_export`, write it to the enrichment cache.

- [ ] **Step 1: Add cache import and store call**

In `stages/s06_normalize.py`, add the import at the top (after the existing imports around line 14):

```python
import cache
import config
```

In the `run()` function, after line 277 (`company.stage = STAGE_PENDING_EXPORT`) and before line 278 (`db.update_company(company)`), insert:

```python
            # Write to shared enrichment cache
            source_run = Path(config.INPUT_PDF).stem
            cache.store(company, source_run)
```

Also add at the top of the file, after the existing imports:

```python
from pathlib import Path
```

- [ ] **Step 2: Verify the store call works in context**

```bash
cd "/Users/antonkozackov/VSCode Projects/SMG/business_data_scraper"
python3 -c "
import cache
cache.init_cache()

# Verify store works with a CompanyRecord that has all fields
from models import CompanyRecord
rec = CompanyRecord(
    name_original='Test Normalize Corp',
    matched_name='Test Normalize Corporation',
    country='DE',
    revenue='5000000',
    employees_count='100',
    ceo_name='John Doe',
    confidence_score=0.85,
)
cache.store(rec, 'test_normalize')

result = cache.lookup('Test Normalize Corp')
assert result is not None
assert result.matched_name == 'Test Normalize Corporation'
assert result.confidence_score == 0.85
print('Normalize-to-cache store: OK')

import os; os.unlink('data/enrichment_cache.db')
"
```

Expected: `Normalize-to-cache store: OK`

- [ ] **Step 3: Commit**

```bash
git add stages/s06_normalize.py
git commit -m "feat: write enriched records to cache after Stage 6 normalization"
```

---

### Task 4: Add `--seed-cache` CLI Command

**Files:**
- Modify: `pipeline.py:41-88` (add CLI argument) and `pipeline.py:186-217` (handle it in `main()`)

- [ ] **Step 1: Add `--seed-cache` argument to `parse_args()`**

In `pipeline.py`, add this argument inside `parse_args()`, after the `--db` argument (after line 86):

```python
    parser.add_argument(
        "--seed-cache",
        metavar="DB_FILE",
        help="Seed the enrichment cache from an existing pipeline database (e.g. data/pipeline.db)",
    )
```

- [ ] **Step 2: Handle `--seed-cache` in `main()`**

In `pipeline.py`, in the `main()` function, after the `--list-layouts` handling block (after line 201) and before the `--fresh` handling, add:

```python
    # Handle --seed-cache
    if args.seed_cache:
        cache.init_cache()
        count = cache.seed_from_db(args.seed_cache)
        print(f"Seeded {count} records from {args.seed_cache} into enrichment cache")
        return
```

- [ ] **Step 3: Test the seed command with existing `pipeline.db`**

```bash
cd "/Users/antonkozackov/VSCode Projects/SMG/business_data_scraper"
python3 pipeline.py --seed-cache data/pipeline.db
```

Expected output: `Seeded 676 records from data/pipeline.db into enrichment cache`

- [ ] **Step 4: Verify the seeded cache has data**

```bash
cd "/Users/antonkozackov/VSCode Projects/SMG/business_data_scraper"
python3 -c "
import cache
cache.init_cache()

# Check a known company from the existing pipeline.db
result = cache.lookup('3M DEUTSCHLAND GMBH')
if result:
    print(f'Found: {result.matched_name}, country={result.country}, revenue={result.revenue}')
else:
    print('Not found — check company names in pipeline.db')

# Count total cached
import sqlite3
conn = sqlite3.connect('data/enrichment_cache.db')
count = conn.execute('SELECT COUNT(*) FROM enriched_companies').fetchone()[0]
print(f'Total cached: {count}')
conn.close()
"
```

Expected: Company found with enriched data, total count ~676

- [ ] **Step 5: Commit**

```bash
git add pipeline.py
git commit -m "feat: add --seed-cache CLI command to populate enrichment cache from existing DB"
```

---

### Task 5: End-to-End Verification

**Files:** None (read-only verification)

- [ ] **Step 1: Seed cache from existing data**

```bash
cd "/Users/antonkozackov/VSCode Projects/SMG/business_data_scraper"
# Remove stale cache if exists from previous tests
rm -f data/enrichment_cache.db
python3 pipeline.py --seed-cache data/pipeline.db
```

Expected: `Seeded 676 records from data/pipeline.db into enrichment cache`

- [ ] **Step 2: Run Boeing Excel pipeline and verify cache hits**

```bash
cd "/Users/antonkozackov/VSCode Projects/SMG/business_data_scraper"
rm -f "data/Boeing suppliers_3_columns.db"
python3 -c "
import asyncio, config, db, cache
from pipeline import apply_overrides, parse_args

# Configure for Boeing
args = parse_args(['--input', 'input/Boeing suppliers_3_columns.xlsx', '--layout', 'boeing_suppliers'])
apply_overrides(args)

# Init
db.init_db()
cache.init_cache()

# Stage 1: Extract
from stages.s01_pdf_extract import run as run_extract
run_extract(country_filter=None)

# Cache lookup (same logic as pipeline.py)
from models import STAGE_PENDING_NORTHDATA, STAGE_PENDING_EXPORT
pending = db.get_pending(STAGE_PENDING_NORTHDATA, limit=100000)
pending_names = [c.name_original for c in pending]
cache_hits = cache.lookup_batch(pending_names)

print(f'Pending companies: {len(pending_names)}')
print(f'Cache hits: {len(cache_hits)}')
if cache_hits:
    print(f'Example hits: {list(cache_hits.keys())[:5]}')

stats = db.get_stats()
print(f'DB stats before cache apply: {stats}')
"
```

Expected: Some cache hits for companies that overlap between Boeing list and the existing 676 enriched companies.

- [ ] **Step 3: Verify cache stats in log output**

Check that the pipeline log shows the cache hit/miss counts. The log line should read something like: `Cache: X companies resolved from cache, Y need enrichment`

- [ ] **Step 4: Clean up test databases**

```bash
cd "/Users/antonkozackov/VSCode Projects/SMG/business_data_scraper"
rm -f "data/Boeing suppliers_3_columns.db"
```
