# Stage 5: Single Enrichment Call + Usage-Limit Rerun Reporting — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Stage 5's 3–5 separate per-company Claude calls with one merged web-search call (CEO + financials + business description), and report how many companies were left un-enriched when a usage limit halts the stage.

**Architecture:** Add a single `enrich_company()` function to `clients/claude_ai.py` that returns CEO, financials, and a business description in one JSON-schema web-search call. Rewrite `stages/s05_ai_enrich.py::_enrich_company()` to call it once (disambiguation stays a separate call before it), applying results with the existing gap-fill guards. Separately, change the Stage 5 loop to enumerate so that when `ClaudeUsageLimitError` fires, all remaining companies are stamped for rerun and a count is logged.

**Tech Stack:** Python 3.13, `claude-agent-sdk` (Agent SDK, not Anthropic API), asyncio, SQLite. **No test suite** — this project has no pytest and `dev-dependencies = []` (per CLAUDE.md). Verification is manual: byte-compile the modules and run the pipeline on a small input. Do **not** add pytest or write test files.

**Verification commands used throughout:**
- Compile check: `uv run python -m py_compile clients/claude_ai.py stages/s05_ai_enrich.py`
- Import check: `uv run python -c "import clients.claude_ai, stages.s05_ai_enrich; print('imports OK')"`

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `clients/claude_ai.py` | Claude Agent SDK wrappers | **Add** `enrich_company()` — one merged web-search call. Leave all existing functions in place. |
| `stages/s05_ai_enrich.py` | Stage 5 orchestration | **Rewrite** `_enrich_company()` to use the single call. **Modify** `run()` loop for usage-limit rerun reporting. |

No other files change. No DB schema, `models.py`, or export changes.

---

## Task 1: Add the merged `enrich_company()` function to `clients/claude_ai.py`

**Files:**
- Modify: `clients/claude_ai.py` (add one new function near the other web-search functions, e.g. after `enrich_missing_financials`)

This function makes ONE `use_web=True` call returning CEO + financials + business
description. It mirrors the existing functions' structure: `@with_retry`
decorator, `_ask_claude(..., use_web=True, output_format=...)`, `_try_parse_json`
with a graceful fallback. It does NOT replace or delete any existing function.

- [ ] **Step 1: Add the function**

Insert the following into `clients/claude_ai.py`. Place it after the
`enrich_missing_financials` function (ends at the line `    return {}` around
line 537) and before `estimate_employee_count`:

```python
@with_retry(
    max_attempts=2, base_delay=2.0, exceptions=(CLIConnectionError, ProcessError, ClaudeTimeoutError)
)
async def enrich_company(
    company_name: str,
    country: str | None = None,
    legal_form: str | None = None,
    known_ceo_name: str | None = None,
    known_ceo_title: str | None = None,
    known_revenue: str | None = None,
    known_employees: str | None = None,
) -> dict:
    """One merged web-search call: CEO + financials + business description.

    Replaces the previous per-company chain of research_ceo/discover_ceo +
    enrich_missing_financials + estimate_employee_count +
    summarize_corporate_structure with a single Agent SDK call. Confirms/enriches
    data we already have and fills gaps; tells Claude to use null for anything it
    cannot verify so good Northdata data is never overwritten by guesses.

    Returns a dict shaped like::

        {
          "ceo": {"name", "title", "linkedin_url", "career_summary"},
          "financials": {"employees_count", "revenue", "total_assets"},
          "business_description": str | None,
          "source_notes": str | None,
        }

    On parse failure, returns the same shape with empty sub-dicts / None values.
    """
    # --- Build conditional context blocks ---
    ceo_context = (
        f'The known managing director / CEO is "{known_ceo_name}"'
        f'{f" ({known_ceo_title})" if known_ceo_title else ""}. '
        "Confirm this person, find their LinkedIn profile URL, and write a "
        "2-3 sentence career summary."
        if known_ceo_name
        else (
            "Find the current CEO, Geschäftsführer, or managing director, their "
            "title, LinkedIn profile URL, and a 2-3 sentence career summary."
        )
    )

    location = f" The company is based in {country}." if country else ""

    kg_hint = ""
    if legal_form and "co. kg" in legal_form.lower():
        kg_hint = (
            " This is a GmbH & Co. KG structure — the actual Geschäftsführer is "
            "typically the managing director of the Komplementär-GmbH "
            "(Verwaltungsgesellschaft / general partner)."
        )

    known_fin = []
    if known_revenue:
        known_fin.append(f"revenue is approximately {known_revenue}")
    if known_employees:
        known_fin.append(f"employee count is approximately {known_employees}")
    known_fin_text = (
        " Already known (do not contradict, only fill what is missing): "
        + "; ".join(known_fin)
        + "."
        if known_fin
        else ""
    )

    prompt = f"""Research the company "{company_name}".{location}{kg_hint}

Gather three things in a single pass:

1. LEADERSHIP: {ceo_context}
2. FINANCIALS: employee count, revenue/turnover (most recent available), and
   total assets if available.{known_fin_text} Look at the company website,
   LinkedIn page, annual reports, business registries, and press releases.
3. BUSINESS DESCRIPTION: a 2-4 sentence professional summary of the business —
   what it does, its scale (revenue/employees), corporate structure, and who
   leads it. Suitable for a business research report.

Only include data you can actually verify from search results. Use null for any
field you cannot find. Do not guess."""

    result = await _ask_claude(
        prompt=prompt,
        use_web=True,
        output_format={
            "type": "json_schema",
            "schema": {
                "type": "object",
                "properties": {
                    "ceo": {
                        "type": "object",
                        "properties": {
                            "name": {"type": ["string", "null"]},
                            "title": {"type": ["string", "null"]},
                            "linkedin_url": {"type": ["string", "null"]},
                            "career_summary": {"type": ["string", "null"]},
                        },
                        "required": ["name", "title", "linkedin_url", "career_summary"],
                        "additionalProperties": False,
                    },
                    "financials": {
                        "type": "object",
                        "properties": {
                            "employees_count": {"type": ["string", "null"]},
                            "revenue": {"type": ["string", "null"]},
                            "total_assets": {"type": ["string", "null"]},
                        },
                        "required": ["employees_count", "revenue", "total_assets"],
                        "additionalProperties": False,
                    },
                    "business_description": {"type": ["string", "null"]},
                    "source_notes": {"type": ["string", "null"]},
                },
                "required": ["ceo", "financials", "business_description", "source_notes"],
                "additionalProperties": False,
            },
        },
    )

    empty = {
        "ceo": {"name": None, "title": None, "linkedin_url": None, "career_summary": None},
        "financials": {"employees_count": None, "revenue": None, "total_assets": None},
        "business_description": None,
        "source_notes": None,
    }

    parsed = _try_parse_json(result)
    if not isinstance(parsed, dict):
        logger.warning("enrich_company: could not parse response for '%s': %s",
                       company_name, (result or "")[:200])
        return empty

    # Merge onto the empty skeleton so callers always get the full shape.
    ceo = parsed.get("ceo") or {}
    fin = parsed.get("financials") or {}
    return {
        "ceo": {
            "name": ceo.get("name"),
            "title": ceo.get("title"),
            "linkedin_url": ceo.get("linkedin_url"),
            "career_summary": ceo.get("career_summary"),
        },
        "financials": {
            "employees_count": fin.get("employees_count"),
            "revenue": fin.get("revenue"),
            "total_assets": fin.get("total_assets"),
        },
        "business_description": parsed.get("business_description"),
        "source_notes": parsed.get("source_notes"),
    }
```

- [ ] **Step 2: Compile and import**

Run:
```bash
uv run python -m py_compile clients/claude_ai.py
uv run python -c "from clients.claude_ai import enrich_company; print('enrich_company OK')"
```
Expected: no output from `py_compile`; prints `enrich_company OK`.

- [ ] **Step 3: Sanity-check the prompt-building branches (no SDK call)**

This verifies the conditional context blocks build without error for the two
CEO branches and the KG hint, by monkeypatching `_ask_claude` to capture the
prompt instead of calling the SDK. Run this one-off command (it does NOT hit the
network):

```bash
uv run python -c "
import asyncio
import clients.claude_ai as c

captured = {}
async def fake_ask(prompt, system_prompt=None, output_format=None, use_web=False):
    captured['prompt'] = prompt
    captured['use_web'] = use_web
    captured['has_schema'] = output_format is not None
    # return a valid JSON string so parsing succeeds
    return '{\"ceo\":{\"name\":\"X\",\"title\":\"CEO\",\"linkedin_url\":null,\"career_summary\":\"s\"},\"financials\":{\"employees_count\":null,\"revenue\":null,\"total_assets\":null},\"business_description\":\"desc\",\"source_notes\":null}'

c._ask_claude = fake_ask

async def main():
    # Branch A: known CEO + KG hint + known financials
    r = await c.enrich_company('Foo GmbH & Co. KG', country='Germany', legal_form='GmbH & Co. KG', known_ceo_name='Jane Doe', known_revenue='10M EUR')
    assert captured['use_web'] is True and captured['has_schema'] is True
    assert 'Jane Doe' in captured['prompt'] and 'Komplementär' in captured['prompt'] and '10M EUR' in captured['prompt']
    assert r['ceo']['name'] == 'X' and r['business_description'] == 'desc'
    # Branch B: discover CEO, no hints
    r2 = await c.enrich_company('Bar Ltd', country='UK')
    assert 'discover' in captured['prompt'].lower() or 'Find the current CEO' in captured['prompt']
    # Parse-failure fallback returns full shape
    async def bad_ask(prompt, system_prompt=None, output_format=None, use_web=False):
        return 'not json at all'
    c._ask_claude = bad_ask
    r3 = await c.enrich_company('Baz')
    assert r3['ceo']['name'] is None and r3['financials']['revenue'] is None and r3['business_description'] is None
    print('prompt-building + fallback OK')

asyncio.run(main())
"
```
Expected: prints `prompt-building + fallback OK` with no assertion error.

- [ ] **Step 4: Commit**

```bash
git add clients/claude_ai.py
git commit -m "feat(claude_ai): add merged enrich_company single-call function

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Rewrite `_enrich_company()` in Stage 5 to use the single call

**Files:**
- Modify: `stages/s05_ai_enrich.py` — replace the body of `_enrich_company()` from
  the line `    cname = company.matched_name or company.name_original` (line ~145)
  through the end of the function (the structure-summary block ending ~line 250).
  The disambiguation block at the top of the function (lines ~137–142) is KEPT
  unchanged.

The new body: one `enrich_company` call, then apply results with the same
gap-fill guards that the four old calls used. Counter semantics preserved:
`ceo_discovered` when a CEO is newly found, `summary_generated` when a career
summary is produced for an already-known CEO, `financials_enriched` when any
financial field is filled.

- [ ] **Step 1: Replace the post-disambiguation body**

In `stages/s05_ai_enrich.py`, the function currently looks like this from the
disambiguation block onward (KEEP the disambiguation block):

```python
    # Task 1: Disambiguation (if northdata returned multiple matches)
    if company.northdata_raw:
        raw = json.loads(company.northdata_raw)
        if raw.get("status") == "multiple" and raw.get("matches"):
            await _disambiguate_company(company, raw["matches"], clie, rate_limiter)
            results["disambiguated"] += 1
```

Replace EVERYTHING after that disambiguation block (i.e. from
`    # Tasks 2 + 3 run in parallel ...` down to the end of `_enrich_company`)
with:

```python
    # Single merged enrichment call: CEO + financials + business description.
    # Replaces the former research_ceo/discover_ceo + enrich_missing_financials
    # + estimate_employee_count + summarize_corporate_structure chain.
    cname = company.matched_name or company.name_original

    had_ceo_name = bool(company.ceo_name)

    try:
        data = await claude_ai.enrich_company(
            company_name=cname,
            country=company.country,
            legal_form=company.legal_form,
            known_ceo_name=company.ceo_name,
            known_ceo_title=company.ceo_current_title,
            known_revenue=company.revenue,
            known_employees=company.employees_count or company.employees_range,
        )
    except claude_ai.ClaudeUsageLimitError:
        raise
    except Exception as e:
        logger.warning("Merged enrichment failed for '%s': %s", company.name_original, e)
        return

    ceo = data.get("ceo") or {}
    fin = data.get("financials") or {}

    # --- Apply CEO ---
    if had_ceo_name:
        # We already had a name (from registry/structure stages): only enrich.
        if ceo.get("career_summary"):
            company.ceo_career_summary = ceo["career_summary"]
        if ceo.get("linkedin_url"):
            company.ceo_linkedin_url = ceo["linkedin_url"]
        if company.ceo_career_summary:
            results["summary_generated"] += 1
    elif ceo.get("name"):
        # Newly discovered CEO.
        company.ceo_name = ceo["name"]
        company.ceo_current_title = ceo.get("title") or "Managing Director"
        company.ceo_career_summary = ceo.get("career_summary")
        company.ceo_linkedin_url = ceo.get("linkedin_url")
        company.ceo_confidence = "medium"
        results["ceo_discovered"] += 1
        logger.info("  Discovered CEO for '%s': %s", company.name_original, company.ceo_name)

    # --- Apply financials (only fill gaps) ---
    filled_financials = False
    if fin.get("revenue") and not company.revenue:
        company.revenue = fin["revenue"]
        if not company.revenue_range:
            company.revenue_range = fin["revenue"]
        filled_financials = True
    if fin.get("employees_count") and not company.employees_count:
        company.employees_count = fin["employees_count"]
        if not company.employees_range:
            company.employees_range = fin["employees_count"]
        filled_financials = True
    if fin.get("total_assets") and not company.total_assets:
        company.total_assets = fin["total_assets"]
        filled_financials = True
    if filled_financials:
        _append_source(company, "claude_web")
        results["financials_enriched"] += 1

    # --- Apply business description -> corporate structure summary (gap-fill) ---
    # S04b may have already written a richer related-entity summary; don't clobber it.
    if not company.corporate_structure_summary and data.get("business_description"):
        company.corporate_structure_summary = data["business_description"]
```

- [ ] **Step 2: Compile and import**

Run:
```bash
uv run python -m py_compile stages/s05_ai_enrich.py
uv run python -c "import stages.s05_ai_enrich; print('s05 import OK')"
```
Expected: no `py_compile` output; prints `s05 import OK`.

- [ ] **Step 3: Verify the apply-logic with a fake enrich_company (no SDK call)**

This drives `_enrich_company` with a stubbed `claude_ai.enrich_company` and a
fake company object to confirm the gap-fill guards behave. Run:

```bash
uv run python -c "
import asyncio, types
import stages.s05_ai_enrich as s5
from clients import claude_ai

class FakeCo:
    def __init__(self, **kw):
        self.name_original='Test Co'; self.matched_name=None; self.country='DE'
        self.legal_form=None; self.northdata_raw=None
        self.ceo_name=None; self.ceo_current_title=None; self.ceo_career_summary=None
        self.ceo_linkedin_url=None; self.ceo_confidence=None
        self.revenue=None; self.revenue_range=None
        self.employees_count=None; self.employees_range=None; self.total_assets=None
        self.corporate_structure_summary=None; self.data_sources_used=None
        for k,v in kw.items(): setattr(self,k,v)

async def fake_enrich(**kwargs):
    return {
        'ceo': {'name':'New Boss','title':'Geschäftsführer','linkedin_url':'https://linkedin.com/in/x','career_summary':'summary'},
        'financials': {'employees_count':'120','revenue':'5M EUR','total_assets':'10M EUR'},
        'business_description':'A maker of widgets.',
        'source_notes':'website',
    }
claude_ai.enrich_company = fake_enrich

async def main():
    # Case 1: no prior data -> CEO discovered, financials filled, description set
    r = {'disambiguated':0,'summary_generated':0,'ceo_discovered':0,'financials_enriched':0,'skipped':0,'error':0}
    c = FakeCo()
    await s5._enrich_company(c, clie=None, rate_limiter=None, results=r)
    assert c.ceo_name=='New Boss' and c.ceo_confidence=='medium', c.ceo_name
    assert c.revenue=='5M EUR' and c.employees_count=='120' and c.total_assets=='10M EUR'
    assert c.corporate_structure_summary=='A maker of widgets.'
    assert r['ceo_discovered']==1 and r['financials_enriched']==1, r
    assert 'claude_web' in (c.data_sources_used or '')

    # Case 2: prior revenue + prior CEO name + prior structure summary -> none overwritten
    r2 = {'disambiguated':0,'summary_generated':0,'ceo_discovered':0,'financials_enriched':0,'skipped':0,'error':0}
    c2 = FakeCo(ceo_name='Old Boss', ceo_current_title='CEO', revenue='99M EUR', corporate_structure_summary='Existing summary from S04b')
    await s5._enrich_company(c2, clie=None, rate_limiter=None, results=r2)
    assert c2.ceo_name=='Old Boss'               # not overwritten
    assert c2.ceo_career_summary=='summary'        # enriched
    assert c2.revenue=='99M EUR'                   # not overwritten
    assert c2.employees_count=='120'               # gap filled
    assert c2.corporate_structure_summary=='Existing summary from S04b'  # S04b wins
    assert r2['summary_generated']==1 and r2['ceo_discovered']==0, r2
    print('apply-logic OK')

asyncio.run(main())
"
```
Expected: prints `apply-logic OK` with no assertion error.

- [ ] **Step 4: Commit**

```bash
git add stages/s05_ai_enrich.py
git commit -m "refactor(s05): use single enrich_company call per company

Replaces the per-company research_ceo/discover_ceo + financials +
employee-estimate + structure-summary chain with one merged web-search
call. Disambiguation stays a separate pre-step. Gap-fill guards preserved
so Northdata/S04b data is never overwritten.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Report un-enriched companies when the usage limit halts Stage 5

**Files:**
- Modify: `stages/s05_ai_enrich.py` — the `results` dict initialization (~line 49),
  the loop header (~line 75), and the `ClaudeUsageLimitError` handler (~lines 100–110).

When the limit fires, stamp the current company AND every remaining company in
the batch for rerun, log a clear count, and surface it in the summary.

- [ ] **Step 1: Add the `skipped_usage_limit` counter**

In `stages/s05_ai_enrich.py::run()`, the results dict is currently:

```python
    results = {
        "disambiguated": 0, "summary_generated": 0, "ceo_discovered": 0,
        "financials_enriched": 0, "skipped": 0, "error": 0,
    }
```

Replace it with:

```python
    results = {
        "disambiguated": 0, "summary_generated": 0, "ceo_discovered": 0,
        "financials_enriched": 0, "skipped": 0, "error": 0,
        "skipped_usage_limit": 0,
    }
```

- [ ] **Step 2: Change the loop header to enumerate**

The loop currently reads:

```python
        for company in companies:
            try:
```

Replace with:

```python
        for index, company in enumerate(companies):
            try:
```

- [ ] **Step 3: Replace the usage-limit handler to stamp remaining + log count**

The handler currently reads:

```python
            except claude_ai.ClaudeUsageLimitError as e:
                logger.error(
                    "Usage limit hit at '%s' (subtype=%s) — stopping Stage 5; "
                    "remaining companies stay at pending_ai for rerun",
                    company.name_original, e.subtype,
                )
                db.mark_for_rerun(
                    company.id, f"usage_limit_reached:{e.subtype}", STAGE_PENDING_AI
                )
                results["error"] += 1
                break
```

Replace the whole `except claude_ai.ClaudeUsageLimitError as e:` block with:

```python
            except claude_ai.ClaudeUsageLimitError as e:
                # Park the current company AND every remaining (unprocessed) company
                # in this batch for rerun, recording the reason on each. They keep
                # their retry budget and stay at pending_ai for the next run.
                remaining = companies[index:]
                marker = f"usage_limit_reached:{e.subtype}"
                for pending in remaining:
                    db.mark_for_rerun(pending.id, marker, STAGE_PENDING_AI)
                results["skipped_usage_limit"] = len(remaining)
                logger.error(
                    "Usage limit reached at '%s' (subtype=%s) — %d of %d companies "
                    "were NOT AI-enriched (left at stage '%s'). Re-run the pipeline "
                    "once the limit resets to finish them.",
                    company.name_original, e.subtype,
                    len(remaining), len(companies), STAGE_PENDING_AI,
                )
                break
```

Note: `companies[index:]` includes the current company (the one that raised), so
it is stamped exactly once — do not also call `mark_for_rerun` separately for it.
`results["error"]` is no longer incremented here; the un-enriched count is tracked
in `skipped_usage_limit` instead.

- [ ] **Step 4: Compile and import**

Run:
```bash
uv run python -m py_compile stages/s05_ai_enrich.py
uv run python -c "import stages.s05_ai_enrich; print('s05 import OK')"
```
Expected: no `py_compile` output; prints `s05 import OK`.

- [ ] **Step 5: Verify the remaining-count + stamping logic (no SDK, no real DB)**

This drives `run()` with a stubbed DB and a `claude_ai.enrich_company` that
raises the usage-limit error on the 2nd company, asserting that companies 2..N
are all stamped and the count is right. Run:

```bash
uv run python -c "
import asyncio
import config
config.INPUT_PDF = 'input/verify.xlsx'  # source_run = Path(config.INPUT_PDF).stem in run()
import stages.s05_ai_enrich as s5
from clients import claude_ai
import db as dbmod

# Build 4 fake companies pending AI
class FakeCo:
    def __init__(self, i):
        self.id=i; self.name_original=f'Co{i}'; self.matched_name=None; self.country='DE'
        self.legal_form=None; self.northdata_raw=None
        self.ceo_name=None; self.ceo_current_title=None; self.ceo_career_summary=None
        self.ceo_linkedin_url=None; self.ceo_confidence=None
        self.revenue=None; self.revenue_range=None
        self.employees_count=None; self.employees_range=None; self.total_assets=None
        self.corporate_structure_summary=None; self.data_sources_used=None
        self.stage='pending_ai'
companies=[FakeCo(i) for i in range(1,5)]

stamped=[]; updated=[]
dbmod.get_pending = lambda stage, limit=100: list(companies)
dbmod.update_company = lambda c: updated.append(c.id)
dbmod.mark_for_rerun = lambda cid, err, stage: stamped.append((cid, err, stage))
dbmod.mark_failed = lambda cid, err: None

import cache as cachemod
cachemod.store = lambda c, run: None

# enrich_company: succeed for Co1, raise usage limit for Co2 onward
async def fake_enrich(**kw):
    name = kw.get('company_name')
    if name == 'Co1':
        return {'ceo':{'name':None,'title':None,'linkedin_url':None,'career_summary':None},
                'financials':{'employees_count':None,'revenue':None,'total_assets':None},
                'business_description':None,'source_notes':None}
    raise claude_ai.ClaudeUsageLimitError('rate_limit')
claude_ai.enrich_company = fake_enrich

asyncio.run(s5.run())

# Co1 processed (advanced); Co2,Co3,Co4 stamped for rerun
assert updated == [1], updated
ids = sorted(cid for cid,_,_ in stamped)
assert ids == [2,3,4], ids
assert all(err=='usage_limit_reached:rate_limit' and stg=='pending_ai' for _,err,stg in stamped)
print('rerun-reporting OK: 3 of 4 parked')
"
```
Expected: prints `rerun-reporting OK: 3 of 4 parked` with no assertion error.

- [ ] **Step 6: Commit**

```bash
git add stages/s05_ai_enrich.py
git commit -m "feat(s05): report un-enriched count + park all remaining on usage limit

When a Claude usage limit halts Stage 5, stamp every remaining company in
the batch for rerun (recording the reason), log 'N of M were NOT AI-enriched',
and surface the count via a skipped_usage_limit summary counter.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: End-to-end manual verification on a small input

**Files:** none (verification only)

Confirms, against the real pipeline, that (a) exactly one Claude call is made per
company in Stage 5, and (b) the usage-limit log line appears correctly if a limit
is hit. This is optional-but-recommended and costs a small amount of usage.

- [ ] **Step 1: Pick the smallest available input**

Run:
```bash
ls -la input/
```
Pick the smallest Excel/PDF (e.g. one of the `wlw_*` files). Note its name and
the matching `--layout` (see `uv run python pipeline.py --list-layouts`).

- [ ] **Step 2: Run the pipeline on it (fresh) and watch Stage 5 logs**

Run (substitute the chosen input + layout):
```bash
uv run python pipeline.py --input "input/<smallest file>" --layout <layout> --fresh 2>&1 | tee /tmp/s5_verify.log
```

- [ ] **Step 3: Confirm one call per company + reporting**

Inspect the log:
```bash
grep -E "Stage 5|ai_enrich complete|Discovered CEO|NOT AI-enriched|Usage limit" /tmp/s5_verify.log
```
Expected observations:
- Stage 5 runs and completes with an `ai_enrich complete ...` summary line.
- If a usage limit is hit, you see the new line: `Usage limit reached at '<name>' ... N of M companies were NOT AI-enriched (left at stage 'pending_ai')`.
- No errors about merged enrichment beyond ordinary per-company warnings.

- [ ] **Step 4 (optional): Confirm parked companies remain rerunnable**

If a limit was hit, re-running the same command (without `--fresh`) should pick
up the `pending_ai` companies and continue Stage 5 from where it stopped:
```bash
uv run python pipeline.py --input "input/<smallest file>" --layout <layout> 2>&1 | grep -E "Stage 5|ai_enrich complete"
```
Expected: Stage 5 reports the previously-parked count as the companies to process.

---

## Self-Review

**Spec coverage:**
- Part 1 (usage-limit reporting): Task 3 — enumerate loop, stamp all remaining, log "N of M", `skipped_usage_limit` counter. ✓
- Part 2 (single call): Task 1 (`enrich_company`) + Task 2 (Stage 5 rewrite). ✓
- "Keep old functions in place": Task 1 explicitly adds without removing; Tasks 2/3 never delete. ✓
- "Disambiguation stays separate": Task 2 Step 1 keeps the disambiguation block, replaces only what follows. ✓
- `business_description` → `corporate_structure_summary` with `if not ...` guard: Task 2 Step 1. ✓
- Counter semantics preserved: Task 2 Step 1 + Step 3 assertions. ✓

**Placeholder scan:** No TBD/TODO; all code blocks complete; verification commands concrete. ✓

**Type consistency:** `enrich_company` return shape (`ceo`/`financials`/`business_description`/`source_notes`) is defined in Task 1 and consumed identically in Task 2. `mark_for_rerun(id, error, stage)` signature matches `db.py`. `STAGE_PENDING_AI` already imported in `s05_ai_enrich.py`. ✓

**No-test-suite honored:** No pytest, no test files added; all verification via `py_compile` + one-off `python -c` scripts (with the SDK/DB stubbed) + a real pipeline run. ✓
