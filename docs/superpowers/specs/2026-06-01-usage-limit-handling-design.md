# Design: Usage-limit handling, Sonnet/medium model, and rerun reset

**Date:** 2026-06-01
**Status:** Approved

## Problem

The last pipeline runs hit a Claude **usage limit**, but it was never recorded.
Two recent runs show the damage:

- **cologne** (`2605 cologne_hvac_master_list_with_categories.db`): all 158 `done`
  rows have zero AI enrichment — no `ceo_career_summary`, no
  `corporate_structure_summary`, no `claude_web` source, no revenue/employees/CEO.
- **wlw** (`wlw_heizung_klima_lueftung_companies_over_20.db`): 738/825 `done` rows
  missing structure summaries, 752 missing CEO summaries; plus 71
  `search_input_not_found` rows stuck at `pending_northdata`.

Root causes:

1. `clients/claude_ai.py` never inspects `ResultMessage.is_error` /
   `AssistantMessage.error`, so a usage limit is invisible — it either surfaces as
   a generic `ProcessError` or returns an error string that silently lands in a
   data field.
2. The per-task `except Exception` blocks in the Claude-calling stages swallow the
   failure (`logger.warning`) and the company still advances to `done` with empty
   enrichment.
3. Northdata `status == "error"` companies dead-end at `db.mark_failed` and never
   reach web enrichment (separate from the clean `not_found` path, which *is*
   routed through fallback → S05 correctly).

Confirmed: web enrichment is **not** structurally skipped for no-Northdata
companies — `not_found` → S03 fallback → `pending_ceo` → … → S05, and S05's web
tasks are gated on missing fields, not on Northdata success. The empty cologne
enrichment is wholesale Claude-call failure (the usage limit), not a skip.

## Decisions (locked)

- **Model/effort:** hardcode `sonnet` + `medium` effort in `claude_ai.py`.
- **Rerun marking:** on usage limit, reset row to its stage's pending state with an
  error note and do **not** increment `retry_count` (never auto-fails from a limit).
- **Missing-data signal (reset):** `corporate_structure_summary` empty OR
  `ceo_career_summary` empty OR `data_sources_used` lacking `claude_web`.
- **Remediation scope:** reset only the two recent DBs (cologne + wlw) now.
- **Northdata error reroute:** treat `status == "error"` like `not_found`
  immediately → `pending_fallback` (accepted tradeoff: loses Northdata retry on
  transient scraping glitches, but no company is denied web enrichment).

## Changes

### 1. Model + effort — `clients/claude_ai.py`
Module constants `_MODEL = "sonnet"`, `_EFFORT = "medium"`, set on
`ClaudeAgentOptions` inside `_ask_claude` (the single chokepoint for all 8 Claude
call types). Fields confirmed present in the installed SDK: `model: str | None`,
`effort: Literal["low","medium","high","max"] | None`.

### 2. Usage-limit detection — `clients/claude_ai.py`
- New `ClaudeUsageLimitError(Exception)` carrying `.subtype`.
- Import `AssistantMessage`. In `_collect()`, detect:
  - `AssistantMessage.error in {"rate_limit", "billing_error"}` (documented enum), and
  - `ResultMessage.is_error` is True (capture `subtype` / `stop_reason`).
- On detection: `logger.error("Claude usage limit reached (subtype=%s)", subtype)`
  and raise `ClaudeUsageLimitError(subtype)`.
- **Not** added to any `with_retry` exception tuple — it propagates immediately
  rather than burning 2–3 retries.

### 3. Propagate + stop — `s02_northdata.py`, `s04b_structure.py`, `s05_ai_enrich.py`
These three stages call Claude. In each:
- The narrow per-call `except Exception` blocks get a preceding
  `except claude_ai.ClaudeUsageLimitError: raise` so the limit is not swallowed.
- The per-company loop gains `except claude_ai.ClaudeUsageLimitError as e:` that
  calls `db.mark_for_rerun(company.id, f"usage_limit_reached:{e.subtype}", <stage>)`
  and **breaks** the loop (once the limit is hit, the rest will fail too).
- Rerun stage per stage: S02 → `pending_northdata`, S04b → `pending_structure`,
  S05 → `pending_ai`.

### 4. db helper — `db.py`
`mark_for_rerun(record_id, error, stage)`: `UPDATE companies SET error=?, stage=?,
updated_at=datetime('now') WHERE id=?` — leaves `retry_count` untouched.

### 5. Northdata error reroute — `s02_northdata.py`
Replace the `status == "error"` branch (`db.mark_failed(...)` + `continue`) with:
set `company.stage = STAGE_PENDING_FALLBACK`, keep the error string in
`company.error` for traceability, fall through to `db.update_company(company)`.

### 6. Reset script — new `reset_for_rerun.py`
CLI utility that, for a given DB path, flips `done` rows back to `pending_ai`
(`error='reset_for_rerun'`) where:
`corporate_structure_summary` empty OR `ceo_career_summary` empty OR
`data_sources_used NOT LIKE '%claude_web%'`. Prints a before/after count. Run
against cologne + wlw now; report counts. Does **not** launch the pipeline.

## Verification

- `uv run python -c "import ..."` for each edited module (syntax/imports).
- `db.mark_for_rerun` exercised by the reset script against the real DBs; confirm
  row counts move from `done` to `pending_ai` via `sqlite3` queries.
- Re-running the pipeline (user-initiated, after the limit resets) re-enriches the
  reset rows; usage-limit detection verified opportunistically if a limit recurs.

## Out of scope

- No test framework is introduced (project has none); verification is via real-DB
  queries and import/syntax checks.
- Retry/backoff tuning for the usage limit (e.g. wait-and-resume) — a possible
  follow-up; for now the limit cleanly stops and marks rows for rerun.
