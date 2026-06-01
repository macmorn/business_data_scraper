# Stage 5: Single Enrichment Call + Usage-Limit Rerun Reporting

## Problem

Two issues in Stage 5 (`stages/s05_ai_enrich.py`):

1. **Silent un-enriched companies on usage limit.** When a Claude usage limit
   (`ClaudeUsageLimitError`) halts Stage 5, the current company is marked for
   rerun and the loop `break`s. Every *remaining* company in the batch silently
   stays at `pending_ai` (correct for reruns) but the count of how many were
   left behind is never reported. After the break, the pipeline proceeds to
   Stage 6/7 with only the companies that got enriched, and the operator has no
   visibility into how much work remains.

2. **Too many Claude SDK calls per company.** Each company triggers **3–5
   separate** Agent SDK calls:

   | Call | Purpose | Web search |
   |------|---------|------------|
   | `research_ceo` *or* `discover_ceo` | CEO name/title/LinkedIn + career summary | yes |
   | `enrich_missing_financials` | revenue / employees / total assets | yes |
   | `estimate_employee_count` | headcount (only if still missing) | yes |
   | `summarize_corporate_structure` | structure narrative | no |

   Each is a separate subprocess + web-search session — slow, and it burns the
   usage limit several times faster than necessary.

## Solution

### Part 1 — Report un-enriched companies when the usage limit halts Stage 5

**Approach: log summary only.** No CSV changes, no DB schema changes. Companies
already correctly remain at `pending_ai` for the next run; this change only
makes the count visible.

In `stages/s05_ai_enrich.py`, when `ClaudeUsageLimitError` is caught in the
per-company loop:

1. Compute the number of un-enriched companies = the current company plus every
   company after it in the `companies` list that was never processed
   (`len(companies) - index_of_current`).
2. Stamp **all** remaining companies (current + the untouched tail) with the
   same rerun marker via `db.mark_for_rerun(id, "usage_limit_reached:<subtype>",
   STAGE_PENDING_AI)`, so the reason is recorded on every parked company, not
   just the current one. This keeps their retry budget intact.
3. Emit a prominent log line, e.g.:

   ```
   ⚠️  Usage limit reached — 18 of 47 companies were NOT AI-enriched
       (left at stage 'pending_ai'). Re-run the pipeline once the limit
       resets to finish them.
   ```

4. Add a `skipped_usage_limit` counter to the `results` dict so it appears in
   `tracker.summary(results)`.

To know which companies are "remaining," the loop is changed to
`for index, company in enumerate(companies):` so the tail can be sliced
(`companies[index + 1:]`) when the limit fires.

### Part 2 — Collapse the per-company Claude calls into one

Add **one** new function to `clients/claude_ai.py`:

```python
async def enrich_company(
    company_name: str,
    country: str | None = None,
    legal_form: str | None = None,
    known_ceo_name: str | None = None,
    known_ceo_title: str | None = None,
    known_revenue: str | None = None,
    known_employees: str | None = None,
) -> dict
```

A single `use_web=True` call with a JSON-schema `output_format` returning:

```json
{
  "ceo": {
    "name": "string or null",
    "title": "string or null",
    "linkedin_url": "string or null",
    "career_summary": "string or null"
  },
  "financials": {
    "employees_count": "string or null",
    "revenue": "string or null",
    "total_assets": "string or null"
  },
  "business_description": "string or null",
  "source_notes": "string or null"
}
```

The prompt folds in the inputs as known context so Claude **confirms/enriches**
existing data and **fills gaps** rather than overwriting good Northdata values:

- **CEO branch (unified):** the single prompt handles both of today's cases. If
  `known_ceo_name` is provided, Claude is told "the known managing director is
  X — confirm and research their background + LinkedIn"; if not, "discover the
  current CEO / Geschäftsführer / managing director." Includes the existing
  GmbH & Co. KG / Komplementär hint when `legal_form` contains "co. kg".
- **Financials branch:** Claude is told what we already know (so it only fills
  missing revenue/employees/assets) and to use `null` for anything it cannot
  verify.
- **business_description:** a 2–4 sentence professional business / corporate
  structure narrative.

Timeout: `_TIMEOUT_WEB` (web-search path). Retry decorator matches the existing
web-search functions (`max_attempts=2`, retry on
`CLIConnectionError, ProcessError, ClaudeTimeoutError`). `ClaudeUsageLimitError`
propagates (never retried), as with the other functions.

Parsing reuses `_try_parse_json` with a graceful fallback to an empty-ish dict
(`{"ceo": {}, "financials": {}, ...}`) on parse failure, mirroring the existing
helpers.

#### Stage 5 rewrite of `_enrich_company()`

1. **Disambiguation first** — unchanged. Still its own `disambiguate()` call,
   still scrapes the chosen Northdata URL. (Cannot be merged: it must complete
   before we know which company to enrich, and it feeds data into the
   enrichment call.)
2. **Single `enrich_company` call** replaces the four separate calls
   (`research_ceo`/`discover_ceo` + `enrich_missing_financials` +
   `estimate_employee_count` + `summarize_corporate_structure`). It is passed
   the post-disambiguation known values (`company.ceo_name`,
   `company.ceo_current_title`, `company.revenue`, `company.employees_count`).
3. **Apply results with the same gap-filling guards already in place:**
   - CEO: only set `ceo_name`/`title`/`linkedin`/`career_summary` from the
     result when we don't already have a name (discovery), or always refresh
     `career_summary`/`linkedin_url` when we already had the name (research).
     Set `ceo_confidence = "medium"` only on newly discovered CEOs.
   - Financials: only set `revenue` if missing; only set `employees_count` if
     missing; set `total_assets` if returned. Append `claude_web` source when
     any financial field is filled.
   - `business_description` → maps to the existing `corporate_structure_summary`
     field, guarded by `if not company.corporate_structure_summary` so S04b's
     richer related-entity summary wins when present. **No new DB column.**
   - Update the `results` counters to reflect what the single call produced,
     preserving today's semantics: increment `ceo_discovered` when a CEO is
     newly found (we had no name before), `summary_generated` when a CEO
     career summary is produced for an already-known CEO, and
     `financials_enriched` when any revenue/employees/assets field is filled.

### What stays unchanged / unremoved

- The old functions `research_ceo`, `discover_ceo`, `enrich_missing_financials`,
  `estimate_employee_count`, `summarize_corporate_structure` **remain in
  `clients/claude_ai.py`** (per user choice). Note `summarize_corporate_structure`
  is **still actively used by `stages/s04b_structure.py`** and could not be
  removed regardless.
- `resolve_company_name` (used by S02), `disambiguate` (S05), and
  `extract_ceo_from_text` are untouched.
- DB schema, `models.py`, and the export stage are untouched.

## Files to Modify

| File | Change |
|------|--------|
| `clients/claude_ai.py` | Add `enrich_company()` single merged web-search function |
| `stages/s05_ai_enrich.py` | Rewrite `_enrich_company()` to use the single call; add usage-limit rerun reporting (enumerate loop, stamp remaining companies, log count, `skipped_usage_limit` counter) |

## Edge Cases

- **Parse failure on the merged call:** falls back to empty sub-dicts; company
  passes through with whatever Northdata/registry data it already had (same
  resilience as today's per-call fallbacks).
- **Disambiguation still hits the limit first:** `disambiguate()` is a non-web
  call made before `enrich_company`; if the limit fires there, the existing
  `ClaudeUsageLimitError` handling in the loop catches it and the company is
  parked for rerun (now with the improved tail reporting).
- **S04b already wrote a structure summary:** the `if not
  company.corporate_structure_summary` guard means the merged call's
  `business_description` only fills the gap, never overwrites S04b's output.
- **Re-run after limit resets:** parked companies at `pending_ai` are re-fetched
  by `db.get_pending(STAGE_PENDING_AI, ...)` on the next run and processed
  normally — the single call now does all enrichment per company.
