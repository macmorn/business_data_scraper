# Stage 2 Login Failure + Stage 5 Empty-Response Handling, Raw Salvage, and Cache Guard

This spec covers two related fixes from the same investigation. Both address the
pipeline silently producing degraded/empty data instead of failing loudly:

- **Part 0 — Northdata login failure** (the primary data source is silently
  unauthenticated when the account is cancelled).
- **Parts A–C — Stage 5 empty-response handling** (AI enrichment silently
  passes empty results through to `done` and poisons the cache).

---

## Part 0 — Northdata login must fail loudly

### Problem

`clients/northdata_browser.py::_login()` is entirely non-fatal. Every failure
path just logs and returns, leaving `self._logged_in = False`, and the pipeline
proceeds to scrape Northdata **as an anonymous free-tier user**:

- email input not found → `logger.error` + `return`
- password input not found → `logger.error` + `return`
- no logout link after submit → `logger.warning("login may have failed")`
- any exception → `logger.error`

The Northdata account was cancelled, so login silently fails and Stage 2 — the
**primary** data source — returns only limited free-tier data (or none). This is
a major contributor to empty output, larger than the Stage 5 issue.

### Decisions (from user)

- **Scope of failure:** *Fail Stage 2 only.* Login failure aborts Stage 2 loudly
  but the pipeline continues to Stage 3 (registry fallback) and beyond, so
  companies still get partial enrichment.
- **Detection:** *Any non-success = failure.* Missing form fields, an exception
  during login, OR no logout link after submit all count as login failure.

### Solution

`clients/northdata_browser.py`:

- Add an exception class:

  ```python
  class NorthdataLoginError(Exception):
      """Raised when premium login was attempted but did not succeed."""
  ```

- Rewrite `_login()` so each non-success path **raises** `NorthdataLoginError`
  with a specific message, instead of logging+returning:
  - email field not found → `raise NorthdataLoginError("email input not found")`
  - password field not found → `raise NorthdataLoginError("password input not found")`
  - after submit, no `logout`/`_logout` in page content →
    `raise NorthdataLoginError("no logout link after submit — credentials rejected (account cancelled?)")`
  - On the happy path, set `self._logged_in = True` as today.
  - Wrap the body so an unexpected Playwright exception is re-raised as
    `NorthdataLoginError(str(e))` (so the caller catches a single type). The
    `finally: await page.close()` stays.
- `start()` is unchanged in structure: it only calls `_login()` when BOTH
  `self._email` and `self._password` are set. So "no credentials configured →
  anonymous browsing" remains legitimate and never raises. When credentials ARE
  set and login fails, `start()` now propagates `NorthdataLoginError`.

`stages/s02_northdata.py::run()`:

- Today: `await client.start()` is inside the `try:` whose `finally` calls
  `client.stop()`. An exception from `start()` would propagate out of `run()` and
  crash the WHOLE pipeline (`pipeline.py` awaits `run_northdata()` with no guard).
- Change: catch `NorthdataLoginError` around `client.start()` specifically, log a
  loud error, and **return early from Stage 2** (pipeline continues). The
  `pending_northdata` companies stay at their stage for a rerun once the account
  works. Concretely:

  ```python
  try:
      await client.start()
  except NorthdataLoginError as e:
      logger.error(
          "Northdata login FAILED (%s) — Stage 2 aborted. Companies remain at "
          "pending_northdata. Fix NORTHDATA_EMAIL/PASSWORD (account may be "
          "cancelled) and re-run. Continuing to fallback stages.", e,
      )
      await client.stop()
      return
  # ... existing per-company loop unchanged, still inside its own try/finally
  ```

  (Implementation detail: restructure so the early-return path closes the client
  and the per-company loop keeps its existing `finally: client.stop()`. Ensure
  `client.stop()` is not double-called.)

### Files (Part 0)

| File | Change |
|------|--------|
| `clients/northdata_browser.py` | Add `NorthdataLoginError`; `_login()` raises on any non-success; `start()` propagates it |
| `stages/s02_northdata.py` | Catch `NorthdataLoginError` at `start()`, log loudly, return early (Stage 2 only) |

### Edge Cases (Part 0)

- **No credentials configured:** `start()` skips `_login()` entirely → no raise →
  anonymous browsing as today. (Legitimate free-tier use.)
- **Cookie-consent / Cloudflare during login:** surfaces as a Playwright
  exception inside `_login()` → re-raised as `NorthdataLoginError` → Stage 2
  aborts loudly (correct: we cannot confirm premium access).
- **Double `stop()`:** guard the restructure so the browser is closed exactly
  once on the login-failure path.

---

## Parts A–C — Stage 5 empty-response handling

### Problem

The latest export had 114 rows with no business info (empty
`corporate_structure_summary`, `ceo_name`, `revenue`, …). Root cause, confirmed
from `output/pipeline.log`, the per-file DB, and the shared cache:

1. **Stage 5 silently passed empty enrichment through to `done`.** A run started
   18:11 (current single-call code) logged, for all 114 companies:

   ```
   WARNING | clients.claude_ai | enrich_company: could not parse response for '<NAME>':
   INFO    | ai_enrich         | [N/114] <name> -> passed through
   ```

   The raw response was **empty/whitespace** (nothing after the colon) — almost
   certainly a soft/degraded usage limit that did NOT raise
   `ClaudeUsageLimitError` (the hard `rate_limit` had hit earlier at 14:20).
   `enrich_company` parsed `""` → empty skeleton → returned WITHOUT raising →
   `_enrich_company` applied nothing → `run()` advanced the company to
   `pending_normalize` → normalize → export → `done`, indistinguishable from a
   company that genuinely has no public data.

2. **The empty records poisoned the shared cache.** `cache.store()` (called from
   s05 and s06) wrote all 114 as empty (`cached_at=19:06`). Verified: 114/114 are
   now in `data/enrichment_cache.db` with zero enrichment. On any future run the
   cache lookup in `pipeline.py` (~line 159) copies the empty fields and jumps
   the company straight to `pending_export`, **bypassing Stage 5 forever**. This
   is why `reset_for_rerun.py` exists and why naive reruns don't recover.

The export schema is NOT at fault — `corporate_structure_summary`, `ceo_name`,
etc. are all present in `CSV_COLUMNS`. The data was never written.

## Goals

1. **Preserve the raw Claude output** from the merged call, even when it isn't
   valid JSON, so nothing the model returned is thrown away.
2. **Salvage business info from non-JSON prose** — when Claude returns text but
   not valid JSON, extract at least a business description (and LinkedIn/CEO
   where cheaply possible) into `corporate_structure_summary`.
3. **Flag genuinely-empty responses for review** instead of exporting them as
   silently-blank `done` rows.
4. **Stop the cache from storing un-enriched records**, so failures are
   re-attempted on the next run rather than locked in permanently.

## Design Decisions (from user)

- **Field:** reuse the existing `corporate_structure_summary` column for the
  salvaged business description (gap-filled only when empty). No new export
  column.
- **Empty response:** pass the company through to `done` (don't block the
  pipeline) BUT set `needs_review_flag = True` and record an error marker so it
  lands in `*_needs_review.csv`.
- **Cache:** guard `cache.store()` to refuse records that failed enrichment.

## Solution

### Part A — `enrich_company` returns and preserves raw output

`clients/claude_ai.py::enrich_company` currently returns a dict and discards the
raw `_ask_claude` text on parse failure. Change it to:

- Add `"raw"` to the returned dict: the verbatim `result` string from
  `_ask_claude` (or `""`). Always present.
- When `_try_parse_json` succeeds → behave as today, plus `"raw"`.
- When parse fails BUT `result` has non-whitespace text → **salvage**:
  - `business_description` ← `_strip_markdown(result)` (truncated to a sane
    length, e.g. 1000 chars), so prose responses still yield a description.
  - `ceo.linkedin_url` ← `_extract_linkedin_url(result)` if present.
  - (CEO name is NOT guessed from prose — too unreliable; left null. This
    matches the existing `discover_ceo` text-fallback behavior.)
  - Return this partially-salvaged dict with `"raw"` set.
- When parse fails AND `result` is empty/whitespace → return the empty skeleton
  with `"raw": ""` (nothing to salvage). Log at WARNING as today.

The return shape becomes:

```json
{
  "ceo": {"name", "title", "linkedin_url", "career_summary"},
  "financials": {"employees_count", "revenue", "total_assets"},
  "business_description": str | None,
  "source_notes": str | None,
  "raw": str
}
```

### Part B — Stage 5 applies salvage, flags empty, stores raw

`stages/s05_ai_enrich.py::_enrich_company`:

- After calling `enrich_company`, compute `raw = data.get("raw") or ""`.
- **Store raw** on the company so it is not lost. The per-file `companies` table
  has no dedicated column for Claude output; rather than a schema migration,
  fold the raw text into the existing `northdata_raw` JSON envelope under a
  `claude_enrich_raw` key (same pattern S05 already uses for `scraped_data` /
  `disambiguation` in `_disambiguate_company`). This keeps it queryable and
  exportable-adjacent without new columns. (If `northdata_raw` is absent, create
  a minimal JSON object `{"claude_enrich_raw": raw}`.)
- **Apply enrichment** with the existing gap-fill guards (CEO, financials,
  `business_description → corporate_structure_summary`), unchanged.
- **Detect "empty/failed" enrichment** = the response produced no usable data:
  no CEO name (newly), no financial field filled, AND no
  `business_description`/salvaged description, AND `raw` is empty/whitespace.
  When this holds:
  - Set `company.needs_review_flag = True`.
  - Set `company.error = "enrichment_empty_response"`.
  - Still allow the normal advance to `pending_normalize` in `run()` (pass
    through), so the pipeline completes — but the row is now flagged.
- A salvaged-prose response (Part A) is NOT treated as empty — it filled
  `corporate_structure_summary`, so the company is genuinely enriched.

`run()` is otherwise unchanged: it still advances non-exception companies to
`pending_normalize`. The usage-limit `break` path (Task 3 from the prior change)
stays as-is for hard `ClaudeUsageLimitError`.

### Part C — Cache guard

`cache.py::store` (and `_store_records`) must refuse to cache un-enriched
records. The cache table has no `stage`/`error`/`needs_review` columns, so the
guard is **content-based**: a record is "enriched enough to cache" if it has any
real enrichment signal beyond bare extraction. Define:

```python
def _is_enriched(record) -> bool:
    # Cache only records that carry actual enrichment, not failed/empty ones.
    return bool(
        (record.corporate_structure_summary or "").strip()
        or (record.ceo_name or "").strip()
        or (record.ceo_career_summary or "").strip()
        or (record.revenue or "").strip()
        or (record.employees_count or "").strip()
        or "claude_web" in (record.data_sources_used or "")
    )
```

- `store()`: skip (and DEBUG-log) records where `_is_enriched` is False.
- `_store_records()`: filter the batch to enriched records; this also protects
  `store_batch` and `seed_from_db`.
- `seed_from_db` keeps reading `stage='done'` rows but now only the enriched
  ones land in the cache.

This stops new empty records from entering the cache. It does NOT delete the 114
already-poisoned rows — see "Existing bad data" below.

### Existing bad data (the 114 already cached + exported)

Out of scope for the code change, handled operationally:

- The 114 empty cache rows (`cached_at=19:06`) will continue to short-circuit
  Stage 5 until removed. After this fix, clear them so they re-enrich:
  - `reset_for_rerun.py` already flips the per-file DB's empty `done` rows back
    to `pending_ai`.
  - The empty **cache** rows must also be deleted (e.g. a small SQL delete using
    the same `_is_enriched`-style predicate, or `DELETE FROM enriched_companies
    WHERE cached_at LIKE '2026-06-01 19:%'`). A follow-up rerun then re-attempts
    enrichment with the fixed logic.
- This spec does not auto-purge on startup (too destructive to do implicitly);
  it will be a documented manual/utility step.

## Files to Modify

| File | Change |
|------|--------|
| `clients/claude_ai.py` | `enrich_company`: add `"raw"` to return; salvage description + LinkedIn from non-JSON prose; empty → skeleton with `raw=""` |
| `stages/s05_ai_enrich.py` | `_enrich_company`: store raw into `northdata_raw` JSON envelope; flag empty-response rows (`needs_review_flag` + `error="enrichment_empty_response"`) |
| `cache.py` | Add `_is_enriched()`; guard `store()` / `_store_records()` to skip un-enriched records |

No DB schema change, no `models.py` change, no `CSV_COLUMNS` change.

## Edge Cases

- **Salvaged prose is long/markdown:** truncate to ~1000 chars and strip
  markdown via the existing `_strip_markdown` helper.
- **A company already has `corporate_structure_summary` from S04b:** the gap-fill
  guard (`if not company.corporate_structure_summary`) means salvage never
  overwrites it.
- **Empty response but company already had registry data (CEO/revenue from
  Northdata):** it is NOT flagged empty (it has real data); it just didn't gain
  AI enrichment. Only flag when there is genuinely nothing.
- **Cache guard false-negative:** a company with only Northdata data (no
  `claude_web`, no summary) won't be cached. That's acceptable — caching exists
  to skip *enrichment*; a Northdata-only record is cheap to re-derive and we'd
  rather re-attempt its AI enrichment than lock in a thin record. (Documented
  trade-off.)
