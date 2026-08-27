# Enrichment Cache Design

## Problem

Each input file (PDF/Excel) gets its own SQLite database. When the same company appears across multiple input files (e.g., "3M" in both Boeing and Airbus lists), it gets enriched separately each time — wasting Northdata/API calls and producing inconsistent data.

## Solution

A shared **enrichment cache** (`data/enrichment_cache.db`) that stores fully enriched company records. Per-file pipeline runs consult the cache before calling external APIs. Cache hits skip enrichment entirely and go straight to export.

## Architecture

### Two-Tier DB System

```
data/enrichment_cache.db     — Shared, persistent. One row per unique company.
data/{input_stem}.db         — Per-file, ephemeral. Pipeline stage state for one run.
```

### Pipeline Flow

```
Input File
    ↓
Stage 1: Extract names → per-file DB (stage="new")
    ↓
Cache Lookup: lookup_batch(names)
    ├─ HIT  → copy enriched data into per-file DB, stage="pending_export"
    └─ MISS → stage="pending_northdata" (normal pipeline)
         ↓
    Stages 2-6: Enrich via Northdata, registries, AI
         ↓
    After Stage 6 (normalize): cache.store(record) — write to cache
         ↓
Stage 7: Export from per-file DB
```

### Cache Lookup Timing

The cache is consulted **once**, immediately after Stage 1 completes. This is implemented as a new step in `s01_pdf_extract.run()` or as a thin wrapper called from `pipeline.py` between Stage 1 and Stage 2.

Companies found in the cache are copied into the per-file DB with all enriched fields populated and `stage="pending_export"`, bypassing Stages 2-6 entirely.

### Cache Write Timing

After Stage 6 (normalize), before advancing a company to `pending_export`, the enriched record is written to the cache via `cache.store(record)`. This ensures only fully processed, normalized records enter the cache.

## Cache DB Schema

**File:** `data/enrichment_cache.db` (SQLite, WAL mode)

**Table:** `enriched_companies`

Same columns as the per-file `companies` table, minus pipeline control fields:

- `name_original TEXT PRIMARY KEY` — exact match lookup key
- All enrichment fields: `matched_name`, `country`, `legal_form`, `status`, `address`, `founded_year`, `register_id`, `lei`, `vat_id`, `revenue`, `employees_count`, `ceo_name`, `officers`, `financials_json`, `northdata_raw`, `corporate_structure_summary`, `confidence_score`, etc.
- `cached_at TEXT DEFAULT CURRENT_TIMESTAMP` — when this record was cached
- `source_run TEXT` — which input file first enriched this company (e.g., "companies.pdf")

**Excluded fields** (pipeline-specific, not cached):
- `stage`, `retry_count`, `error`, `needs_review_flag`

## New Module: `cache.py`

### Functions

```python
def init_cache() -> None
```
Create `enrichment_cache.db` and the `enriched_companies` table if they don't exist.

```python
def lookup(name: str) -> CompanyRecord | None
```
Exact match by `name_original`. Returns a populated `CompanyRecord` or `None`.

```python
def lookup_batch(names: list[str]) -> dict[str, CompanyRecord]
```
Bulk lookup. Returns `{name_original: CompanyRecord}` for all cache hits. Used after Stage 1 for efficiency.

```python
def store(record: CompanyRecord, source_run: str) -> None
```
Upsert a fully enriched record into the cache. Called after Stage 6.

```python
def store_batch(records: list[CompanyRecord], source_run: str) -> None
```
Bulk upsert. Used by the seed command.

```python
def seed_from_db(db_path: str, source_run: str) -> int
```
One-time migration: reads all `stage="done"` records from an existing per-file DB and writes them to the cache. Returns count of records seeded.

## Integration Points

### `pipeline.py`

1. Call `cache.init_cache()` at startup alongside `db.init_db()`.
2. After Stage 1, call cache lookup and route hits to `pending_export`.
3. Add `--seed-cache` CLI flag: seeds cache from an existing DB file.

```python
# After Stage 1 extraction
from cache import lookup_batch, store
hits = lookup_batch(all_extracted_names)
# Copy hit data into per-file DB, set stage=pending_export
# Log: "X companies resolved from cache, Y need enrichment"
```

### `stages/s06_normalize.py`

After normalizing a company and before advancing to `pending_export`, call `cache.store(record)`.

### CLI

```bash
# Seed cache from existing pipeline.db
python pipeline.py --seed-cache data/pipeline.db

# Normal run — cache is consulted automatically
python pipeline.py --input "input/Boeing suppliers_3_columns.xlsx" --layout boeing_suppliers
```

## Seeding from Existing Data

The existing `data/pipeline.db` has 676 fully enriched companies. A `--seed-cache` CLI command migrates them:

```bash
python pipeline.py --seed-cache data/pipeline.db
```

This reads all records with `stage="done"` from the specified DB and inserts them into `enrichment_cache.db`. The `source_run` value is derived from the DB filename stem (e.g., `data/pipeline.db` → `"pipeline"`, `data/A220-Program-Approved-Supplier-List.db` → `"A220-Program-Approved-Supplier-List"`).

## Matching Strategy

**Exact match only** on `name_original` (case-sensitive, as stored). No fuzzy matching, no suffix stripping. This avoids false positives and keeps the implementation simple.

## Edge Cases

- **Cache staleness:** No TTL or expiry. Cached data is treated as permanent. If re-enrichment is needed, delete the cache row or the entire cache file.
- **Partial enrichment:** Only records that complete Stage 6 (normalize) get cached. Failed or in-progress records are never cached.
- **Name collisions:** The same company name from different input files maps to the same cache entry. This is intentional — "3M" from Boeing and "3M" from Airbus get the same enriched data.
- **`--fresh` flag:** The existing `--fresh` flag deletes the per-file DB only, not the cache. To clear the cache, delete `data/enrichment_cache.db` manually.

## Files to Create/Modify

| File | Change |
|------|--------|
| `cache.py` (new) | Cache module with init, lookup, store, seed functions |
| `pipeline.py` | Add `cache.init_cache()`, post-Stage-1 cache lookup, `--seed-cache` CLI flag |
| `stages/s06_normalize.py` | Add `cache.store()` call after normalization |
