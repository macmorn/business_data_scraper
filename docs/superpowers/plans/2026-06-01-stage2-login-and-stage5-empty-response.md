# Stage 2 Login Failure + Stage 5 Empty-Response Fixes — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Northdata login fail loudly (Stage 2 only) instead of silently scraping anonymously, and stop Stage 5 from exporting empty AI responses as `done` / poisoning the cache — salvaging prose into `corporate_structure_summary`, flagging genuinely-empty responses, and guarding cache writes. Then non-destructively recover the cologne run.

**Architecture:** Three code areas. (1) `clients/northdata_browser.py`: a `NorthdataLoginError` raised on any login non-success; `stages/s02_northdata.py` catches it and returns early. (2) `clients/claude_ai.py`: `enrich_company` returns/salvages raw output; `stages/s05_ai_enrich.py` stores raw, applies salvage, flags empty. (3) `cache.py`: a content-based `_is_enriched` guard on writes. Finally an operational recovery step (no code) for the cologne data.

**Tech Stack:** Python 3.13, Playwright, claude-agent-sdk, SQLite. **No test suite** (CLAUDE.md; pytest not installed, `dev-dependencies = []`). Verification is manual: `py_compile` + import + stubbed `python -c` checks. Do NOT add pytest or test files.

**Verification commands used throughout:**
- Compile: `uv run python -m py_compile <files>`
- Import: `uv run python -c "import <modules>; print('imports OK')"`
- (All stubbed checks pipe `2>&1 | grep -v "^warning:"` to drop uv/venv noise.)

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `clients/northdata_browser.py` | Playwright Northdata client | Add `NorthdataLoginError`; `_login()` raises on any non-success |
| `stages/s02_northdata.py` | Stage 2 orchestration | Catch `NorthdataLoginError` at `start()`, log loudly, return early |
| `clients/claude_ai.py` | Claude SDK wrappers | `enrich_company`: add `"raw"`, salvage prose, empty → skeleton+`raw=""` |
| `stages/s05_ai_enrich.py` | Stage 5 orchestration | Store raw into `northdata_raw` envelope; flag empty-response rows |
| `cache.py` | Shared enrichment cache | `_is_enriched()` guard on `store()` / `_store_records()` |

No DB schema, `models.py`, or `CSV_COLUMNS` change.

---

## Task 1: Northdata login raises on failure

**Files:**
- Modify: `clients/northdata_browser.py` (add exception class ~line 12; rewrite `_login()` lines 101–150)

- [ ] **Step 1: Add the exception class**

In `clients/northdata_browser.py`, after the imports / before `_SKIP_HREFS`
(around line 12), add:

```python
class NorthdataLoginError(Exception):
    """Raised when premium login was attempted but did not succeed.

    Only raised when credentials were provided (NORTHDATA_EMAIL/PASSWORD). A
    failed login means we'd otherwise silently scrape as an anonymous free-tier
    user — likely a cancelled/expired account.
    """
```

- [ ] **Step 2: Rewrite `_login()` to raise on every non-success**

Replace the entire current `_login()` method (lines 101–150) with:

```python
    async def _login(self) -> None:
        """Log in to Northdata with premium credentials.

        Raises NorthdataLoginError on ANY non-success (missing form fields,
        unexpected exception, or no logout link after submit).
        """
        page = await self._context.new_page()
        try:
            await page.goto("https://www.northdata.com/_login", wait_until="domcontentloaded", timeout=30000)
            await _human_delay(page, 1500, 3000)
            await _human_mouse_move(page)

            email_input = await page.query_selector('input[name="email"], input[type="email"], input[name="username"]')
            if not email_input:
                raise NorthdataLoginError("email input not found on login page")

            await email_input.click()
            await _human_delay(page, 200, 500)
            await page.keyboard.type(self._email, delay=random.randint(20, 60))

            pw_input = await page.query_selector('input[name="password"], input[type="password"]')
            if not pw_input:
                raise NorthdataLoginError("password input not found on login page")

            await pw_input.click()
            await _human_delay(page, 200, 500)
            await page.keyboard.type(self._password, delay=random.randint(25, 70))
            await _human_delay(page, 300, 800)

            submit_btn = await page.query_selector('button[type="submit"]')
            if submit_btn:
                await _human_mouse_move(page)
                await submit_btn.click()
            else:
                await pw_input.press("Enter")

            await _human_delay(page, 2500, 4000)

            content = await page.content()
            if "logout" in content.lower() or "_logout" in content:
                self._logged_in = True
                logger.info("Successfully logged in to Northdata as %s", self._email)
            else:
                raise NorthdataLoginError(
                    "no logout link after submit — credentials rejected "
                    "(account cancelled/expired?)"
                )
        except NorthdataLoginError:
            raise
        except Exception as e:
            # Any other failure (navigation, Cloudflare, selector timeout) is
            # also a login failure — we cannot confirm premium access.
            raise NorthdataLoginError(f"login error: {e}") from e
        finally:
            await page.close()
```

Note: `start()` is unchanged — it only calls `_login()` when both `self._email`
and `self._password` are set, so "no credentials → anonymous" never raises.

- [ ] **Step 3: Compile + import**

Run:
```bash
uv run python -m py_compile clients/northdata_browser.py
uv run python -c "from clients.northdata_browser import NorthdataClient, NorthdataLoginError; print('northdata OK')" 2>&1 | grep -v "^warning:"
```
Expected: prints `northdata OK`.

- [ ] **Step 4: Verify each non-success path raises (stubbed page, no real browser)**

This monkeypatches a fake `_context.new_page()` to simulate the three failure
modes plus success, asserting `_login` raises/sets the flag correctly. Run:

```bash
uv run python -c "
import asyncio
from clients.northdata_browser import NorthdataClient, NorthdataLoginError

class FakePage:
    def __init__(self, scenario): self.s=scenario
    async def goto(self,*a,**k): 
        if self.s=='goto_raises': raise RuntimeError('cloudflare')
    async def query_selector(self, sel):
        # email/password inputs
        if 'email' in sel: return None if self.s=='no_email' else FakeInput()
        if 'password' in sel: return None if self.s=='no_pw' else FakeInput()
        if 'submit' in sel: return FakeInput()
        return None
    async def content(self): return '<a href=\"/_logout\">logout</a>' if self.s=='ok' else '<html>login</html>'
    async def close(self): pass
class FakeInput:
    async def click(self): pass
    async def press(self,*a): pass
class FakeKbd:
    async def type(self,*a,**k): pass
class FakeMouse:
    async def move(self,*a,**k): pass
class FakeCtx:
    def __init__(self,scn): self.scn=scn
    async def new_page(self): 
        p=FakePage(self.scn); p.keyboard=FakeKbd(); p.mouse=FakeMouse(); return p
# silence human-delay/mouse helpers
import clients.northdata_browser as nb
async def _noop(*a,**k): pass
nb._human_delay=_noop; nb._human_mouse_move=_noop

async def run_login(scn):
    c=NorthdataClient(email='e',password='p'); c._context=FakeCtx(scn)
    await c._login(); return c._logged_in

async def main():
    # success
    assert await run_login('ok') is True
    # the three failure modes all raise
    for scn in ('no_email','no_pw','rejected','goto_raises'):
        try:
            await run_login(scn); assert False, f'{scn} did not raise'
        except NorthdataLoginError: pass
    print('login-raise paths OK')
asyncio.run(main())
" 2>&1 | grep -v "^warning:"
```
Expected: prints `login-raise paths OK`.

- [ ] **Step 5: Commit**

```bash
git add clients/northdata_browser.py
git commit -m "feat(northdata): raise NorthdataLoginError on any login failure

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Stage 2 catches login failure and aborts Stage 2 only

**Files:**
- Modify: `stages/s02_northdata.py` — import the exception (line 12 area); restructure `run()` lines 45–46 so `start()` is outside the per-company loop's try/finally.

- [ ] **Step 1: Import `NorthdataLoginError`**

Change the import at line 12:

```python
from clients.northdata_browser import NorthdataClient
```
to:
```python
from clients.northdata_browser import NorthdataClient, NorthdataLoginError
```

- [ ] **Step 2: Restructure `run()` so login-failure returns early (Stage 2 only)**

The current code (lines 45–48) is:

```python
    try:
        await client.start()

        for company in companies:
```

Replace it with (pull `start()` into its own guarded block, then open the
loop's own try/finally):

```python
    try:
        await client.start()
    except NorthdataLoginError as e:
        logger.error(
            "Northdata login FAILED (%s) — Stage 2 aborted. %d companies remain "
            "at pending_northdata. Fix NORTHDATA_EMAIL/PASSWORD (the account may "
            "be cancelled/expired) and re-run. Continuing to fallback stages.",
            e, len(companies),
        )
        await client.stop()
        return

    try:
        for company in companies:
```

This leaves the existing per-company loop body unchanged, still terminated by
the original `finally: await client.stop()` at line ~126–127. The browser is
now closed exactly once on each path (early-return closes it and returns before
the loop's try is entered).

- [ ] **Step 3: Compile + import**

Run:
```bash
uv run python -m py_compile stages/s02_northdata.py
uv run python -c "import stages.s02_northdata; print('s02 OK')" 2>&1 | grep -v "^warning:"
```
Expected: prints `s02 OK`.

- [ ] **Step 4: Verify login-failure aborts Stage 2 without crashing (stubbed)**

Drive `run()` with a client whose `start()` raises `NorthdataLoginError`, and a
stubbed DB; assert `run()` returns normally (no exception escapes), `stop()` is
called once, and no company is mutated. Run:

```bash
uv run python -c "
import asyncio
import config
config.NORTHDATA_EMAIL='e'; config.NORTHDATA_PASSWORD='p'; config.NORTHDATA_RETRY_ATTEMPTS=2
config.NORTHDATA_DELAY_MIN=0.0; config.NORTHDATA_DELAY_MAX=0.0
import stages.s02_northdata as s2
from clients.northdata_browser import NorthdataLoginError

stop_calls=[]
class FakeClient:
    def __init__(self,*a,**k): pass
    async def start(self): raise NorthdataLoginError('account cancelled')
    async def stop(self): stop_calls.append(1)
s2.NorthdataClient=FakeClient

import db as dbmod
class C:
    def __init__(self,i): self.id=i; self.name_original=f'C{i}'; self.stage='pending_northdata'; self.address=None
companies=[C(1),C(2)]
dbmod.get_pending=lambda stage,limit=100: list(companies)
updated=[]; dbmod.update_company=lambda c: updated.append(c.id)

asyncio.run(s2.run())   # must NOT raise
assert stop_calls==[1], stop_calls          # stop called exactly once
assert updated==[], updated                  # no company processed/mutated
print('s02 login-abort OK: returned cleanly, stop once, no mutations')
" 2>&1 | grep -v "^warning:"
```
Expected: prints `s02 login-abort OK: returned cleanly, stop once, no mutations`.

- [ ] **Step 5: Commit**

```bash
git add stages/s02_northdata.py
git commit -m "feat(s02): abort Stage 2 loudly on Northdata login failure

Catches NorthdataLoginError at client.start(), logs a clear error, closes
the browser, and returns early so the pipeline continues to fallback/AI.
pending_northdata companies wait for a rerun once the account works.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: `enrich_company` returns + salvages raw output

**Files:**
- Modify: `clients/claude_ai.py` — the tail of `enrich_company` (the parse/return block added previously)

- [ ] **Step 1: Add `"raw"` + prose-salvage to the return logic**

In `clients/claude_ai.py::enrich_company`, the current tail is:

```python
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

Replace that whole block with:

```python
    raw = result or ""

    empty = {
        "ceo": {"name": None, "title": None, "linkedin_url": None, "career_summary": None},
        "financials": {"employees_count": None, "revenue": None, "total_assets": None},
        "business_description": None,
        "source_notes": None,
        "raw": raw,
    }

    parsed = _try_parse_json(result)
    if not isinstance(parsed, dict):
        # Parse failed. If there is non-empty prose, salvage a business
        # description (+ LinkedIn) from it instead of discarding everything.
        if raw.strip():
            salvaged = _strip_markdown(raw)
            if len(salvaged) > 1000:
                salvaged = salvaged[:997] + "..."
            logger.info(
                "enrich_company: non-JSON response for '%s' — salvaged %d chars of prose",
                company_name, len(salvaged),
            )
            return {
                "ceo": {
                    "name": None, "title": None,
                    "linkedin_url": _extract_linkedin_url(raw),
                    "career_summary": None,
                },
                "financials": {"employees_count": None, "revenue": None, "total_assets": None},
                "business_description": salvaged or None,
                "source_notes": None,
                "raw": raw,
            }
        logger.warning(
            "enrich_company: empty/unparseable response for '%s' (len=%d)",
            company_name, len(raw),
        )
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
        "raw": raw,
    }
```

- [ ] **Step 2: Compile + import**

Run:
```bash
uv run python -m py_compile clients/claude_ai.py
uv run python -c "from clients.claude_ai import enrich_company; print('enrich OK')" 2>&1 | grep -v "^warning:"
```
Expected: prints `enrich OK`.

- [ ] **Step 3: Verify raw + salvage + empty branches (stubbed SDK)**

```bash
uv run python -c "
import asyncio
import clients.claude_ai as c

async def main():
    # 1) valid JSON -> raw present, fields parsed
    async def ok(*a,**k): return '{\"ceo\":{\"name\":\"X\",\"title\":\"CEO\",\"linkedin_url\":null,\"career_summary\":\"s\"},\"financials\":{\"employees_count\":null,\"revenue\":null,\"total_assets\":null},\"business_description\":\"desc\",\"source_notes\":null}'
    c._ask_claude=ok
    r=await c.enrich_company('A'); assert r['raw'] and r['ceo']['name']=='X' and r['business_description']=='desc', r

    # 2) non-JSON prose -> salvaged into business_description, linkedin extracted, raw kept
    async def prose(*a,**k): return 'Acme GmbH is a leading widget maker. CEO profile: https://linkedin.com/in/jane'
    c._ask_claude=prose
    r=await c.enrich_company('B')
    assert r['business_description'] and 'widget maker' in r['business_description'], r
    assert r['ceo']['linkedin_url']=='https://linkedin.com/in/jane', r
    assert r['raw'].startswith('Acme'), r

    # 3) empty -> skeleton with raw=''
    async def empty(*a,**k): return '   '
    c._ask_claude=empty
    r=await c.enrich_company('C')
    assert r['business_description'] is None and r['ceo']['name'] is None and r['raw']=='   ', r
    print('enrich_company raw/salvage/empty OK')
asyncio.run(main())
" 2>&1 | grep -v "^warning:"
```
Expected: prints `enrich_company raw/salvage/empty OK`.

- [ ] **Step 4: Commit**

```bash
git add clients/claude_ai.py
git commit -m "feat(claude_ai): enrich_company returns raw + salvages prose responses

Adds 'raw' to the return; on JSON parse failure with non-empty prose,
salvages a business description (+ LinkedIn) via the existing markdown/url
helpers instead of discarding the response. Empty responses return the
skeleton with raw=''.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Stage 5 stores raw + flags empty responses

**Files:**
- Modify: `stages/s05_ai_enrich.py::_enrich_company` — the merged-call block (after disambiguation)

- [ ] **Step 1: Store raw, apply salvage, flag empty**

In `_enrich_company`, the current merged-call block starts at
`data = await claude_ai.enrich_company(...)` and ends at the
`business_description` gap-fill. Locate this trailing section:

```python
    # --- Apply business description -> corporate structure summary (gap-fill) ---
    # S04b may have already written a richer related-entity summary; don't clobber it.
    if not company.corporate_structure_summary and data.get("business_description"):
        company.corporate_structure_summary = data["business_description"]
```

Replace it with:

```python
    # --- Apply business description -> corporate structure summary (gap-fill) ---
    # S04b may have already written a richer related-entity summary; don't clobber it.
    if not company.corporate_structure_summary and data.get("business_description"):
        company.corporate_structure_summary = data["business_description"]

    # --- Preserve the raw Claude output (don't lose it on parse failure) ---
    raw = (data.get("raw") or "").strip()
    if raw:
        try:
            envelope = json.loads(company.northdata_raw) if company.northdata_raw else {}
            if not isinstance(envelope, dict):
                envelope = {"_prev": company.northdata_raw}
        except (json.JSONDecodeError, TypeError):
            envelope = {"_prev": company.northdata_raw}
        envelope["claude_enrich_raw"] = data.get("raw")
        company.northdata_raw = json.dumps(envelope, ensure_ascii=False)

    # --- Flag genuinely-empty enrichment for manual review ---
    # "Empty" = this call produced no usable data AND there was no raw prose to
    # salvage. A company that already had registry data (CEO/revenue from
    # Northdata) is NOT flagged — only ones left with nothing.
    produced_nothing = (
        not data.get("business_description")
        and not (data.get("ceo") or {}).get("name")
        and not (data.get("ceo") or {}).get("career_summary")
        and not any((data.get("financials") or {}).values())
        and not raw
    )
    has_any_real_data = bool(
        (company.corporate_structure_summary or "").strip()
        or (company.ceo_name or "").strip()
        or (company.revenue or "").strip()
        or (company.employees_count or "").strip()
    )
    if produced_nothing and not has_any_real_data:
        company.needs_review_flag = True
        company.error = "enrichment_empty_response"
        logger.warning(
            "  Empty AI response for '%s' — flagged for review (no usable data)",
            company.name_original,
        )
```

- [ ] **Step 2: Compile + import**

Run:
```bash
uv run python -m py_compile stages/s05_ai_enrich.py
uv run python -c "import stages.s05_ai_enrich; print('s05 OK')" 2>&1 | grep -v "^warning:"
```
Expected: prints `s05 OK`.

- [ ] **Step 3: Verify raw-store + empty-flag + no-flag-when-data (stubbed)**

```bash
uv run python -c "
import asyncio, json
import stages.s05_ai_enrich as s5
from clients import claude_ai

class FakeCo:
    def __init__(self, **kw):
        self.name_original='T'; self.matched_name=None; self.country='DE'; self.legal_form=None
        self.northdata_raw=None
        self.ceo_name=None; self.ceo_current_title=None; self.ceo_career_summary=None
        self.ceo_linkedin_url=None; self.ceo_confidence=None
        self.revenue=None; self.revenue_range=None
        self.employees_count=None; self.employees_range=None; self.total_assets=None
        self.corporate_structure_summary=None; self.data_sources_used=None
        self.needs_review_flag=False; self.error=None
        for k,v in kw.items(): setattr(self,k,v)

async def main():
    # A) empty response, no prior data -> flagged, raw NOT stored (raw empty)
    async def empty(**k): return {'ceo':{'name':None,'title':None,'linkedin_url':None,'career_summary':None},'financials':{'employees_count':None,'revenue':None,'total_assets':None},'business_description':None,'source_notes':None,'raw':''}
    claude_ai.enrich_company=empty
    r={'disambiguated':0,'summary_generated':0,'ceo_discovered':0,'financials_enriched':0,'skipped':0,'error':0}
    c=FakeCo(); await s5._enrich_company(c,clie=None,rate_limiter=None,results=r)
    assert c.needs_review_flag is True and c.error=='enrichment_empty_response', (c.needs_review_flag,c.error)
    assert c.northdata_raw is None  # nothing to store

    # B) prose response -> description set, raw stored, NOT flagged
    async def prose(**k): return {'ceo':{'name':None,'title':None,'linkedin_url':None,'career_summary':None},'financials':{'employees_count':None,'revenue':None,'total_assets':None},'business_description':'A widget maker.','source_notes':None,'raw':'A widget maker. raw text'}
    claude_ai.enrich_company=prose
    c2=FakeCo(); await s5._enrich_company(c2,clie=None,rate_limiter=None,results={'disambiguated':0,'summary_generated':0,'ceo_discovered':0,'financials_enriched':0,'skipped':0,'error':0})
    assert c2.corporate_structure_summary=='A widget maker.'
    assert c2.needs_review_flag is False and c2.error is None
    env=json.loads(c2.northdata_raw); assert env['claude_enrich_raw'].startswith('A widget maker'), env

    # C) empty response BUT company already had Northdata revenue -> NOT flagged
    async def empty2(**k): return {'ceo':{'name':None,'title':None,'linkedin_url':None,'career_summary':None},'financials':{'employees_count':None,'revenue':None,'total_assets':None},'business_description':None,'source_notes':None,'raw':''}
    claude_ai.enrich_company=empty2
    c3=FakeCo(revenue='5M EUR'); await s5._enrich_company(c3,clie=None,rate_limiter=None,results={'disambiguated':0,'summary_generated':0,'ceo_discovered':0,'financials_enriched':0,'skipped':0,'error':0})
    assert c3.needs_review_flag is False and c3.error is None, (c3.needs_review_flag,c3.error)
    print('s05 raw-store/empty-flag OK')
asyncio.run(main())
" 2>&1 | grep -v "^warning:"
```
Expected: prints `s05 raw-store/empty-flag OK`.

- [ ] **Step 4: Commit**

```bash
git add stages/s05_ai_enrich.py
git commit -m "feat(s05): store raw Claude output + flag empty AI responses

Folds the raw enrich_company output into the northdata_raw JSON envelope
(no schema change). When the call produces no usable data and there was no
prose to salvage AND the company has no other real data, set needs_review
+ error='enrichment_empty_response' so the row surfaces in the review CSV
instead of exporting as a silently-blank 'done'.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Cache write-guard (stop storing un-enriched records)

**Files:**
- Modify: `cache.py` — add `_is_enriched()`; guard `store()` and `_store_records()`

- [ ] **Step 1: Add `_is_enriched` + guard the writes**

In `cache.py`, add this helper just above `def store(` (line ~205):

```python
def _is_enriched(record: CompanyRecord) -> bool:
    """True if the record carries real enrichment worth caching.

    Failed/empty enrichment (e.g. an empty Claude response) must NOT be cached:
    a cached empty row would short-circuit Stage 5 on future runs. Content-based
    because the cache table has no stage/error/needs_review columns.
    """
    return bool(
        (record.corporate_structure_summary or "").strip()
        or (record.ceo_name or "").strip()
        or (record.ceo_career_summary or "").strip()
        or (record.revenue or "").strip()
        or (record.employees_count or "").strip()
        or "claude_web" in (record.data_sources_used or "")
    )
```

Then change `store()` (currently `_store_records([record], source_run)`) to:

```python
def store(record: CompanyRecord, source_run: str) -> None:
    """Upsert a fully enriched company record into the cache.

    Records that failed/empty enrichment are skipped (not cached), so they are
    re-attempted on the next run rather than locked in.
    """
    if not _is_enriched(record):
        logger.debug("Cache: skipping un-enriched record '%s'", record.name_original)
        return
    _store_records([record], source_run)
```

And make `_store_records()` filter the batch (protects `store_batch` /
`seed_from_db` too). Change its top:

```python
def _store_records(records: list[CompanyRecord], source_run: str) -> None:
    """Internal: upsert records into the cache table (enriched records only)."""
    records = [r for r in records if _is_enriched(r)]
    if not records:
        return
    cols = _ENRICHMENT_COLUMNS + ["source_run"]
```
(the rest of `_store_records` is unchanged.)

- [ ] **Step 2: Compile + import**

Run:
```bash
uv run python -m py_compile cache.py
uv run python -c "import cache; print('cache OK')" 2>&1 | grep -v "^warning:"
```
Expected: prints `cache OK`.

- [ ] **Step 3: Verify the guard (stubbed conn, no real DB write)**

```bash
uv run python -c "
import cache
from models import CompanyRecord

# Capture whether _store_records actually executes inserts
executed=[]
import sqlite3
class FakeConn:
    def execute(self,*a,**k): executed.append(a)
    def __enter__(self): return self
    def __exit__(self,*a): return False
cache._get_cache_conn=lambda: FakeConn()

# 1) un-enriched record -> store() skips, nothing executed
empty=CompanyRecord(name_original='X')  # all enrichment None
cache.store(empty,'run')
assert executed==[], 'un-enriched should not be cached'

# 2) enriched record -> store() writes
enr=CompanyRecord(name_original='Y', ceo_name='Jane Doe')
cache.store(enr,'run')
assert len(executed)>=1, 'enriched record should be cached'

# 3) batch filters mixed input
executed.clear()
cache._store_records([CompanyRecord(name_original='a'), CompanyRecord(name_original='b', revenue='1M')], 'run')
assert len(executed)==1, executed
print('cache guard OK')
" 2>&1 | grep -v "^warning:"
```
Expected: prints `cache guard OK`.

- [ ] **Step 4: Commit**

```bash
git add cache.py
git commit -m "feat(cache): refuse to cache un-enriched records

Adds content-based _is_enriched() guard to store()/_store_records() so
failed/empty enrichment is not written to the shared cache (which would
short-circuit Stage 5 on future runs). Stops new cache poisoning.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: Non-destructive cologne recovery (operational, no code)

**Files:** none (data recovery). REVERSIBLE / preview-first. Run only after Tasks 1–5 are committed.

This resets the 114 empty cologne `done` rows to `pending_ai` so a rerun
re-enriches them. It does NOT delete any cache row — the Northdata data in the
cache (113/113 have `northdata_raw`+`address`) is preserved and overwritten with
real data when the rerun re-caches enriched records.

- [ ] **Step 1: Preview the reset (dry-run, changes nothing)**

Run:
```bash
uv run python reset_for_rerun.py --dry-run "data/2605 cologne_hvac_master_list_with_categories.db"
```
Expected: prints something like `... would reset 114/161 done rows -> pending_ai`.

- [ ] **Step 2: Confirm the count looks right, then execute**

If the dry-run count is ~114, run for real:
```bash
uv run python reset_for_rerun.py "data/2605 cologne_hvac_master_list_with_categories.db"
```
Expected: `... reset 114/161 done rows -> pending_ai`.

- [ ] **Step 3: Verify DB state (Northdata data preserved, stage flipped)**

```bash
uv run python -c "
import sqlite3
c=sqlite3.connect('data/2605 cologne_hvac_master_list_with_categories.db'); c.row_factory=sqlite3.Row
for r in c.execute('SELECT stage, COUNT(*) n FROM companies GROUP BY stage ORDER BY n DESC'):
    print(' ', r['stage'], r['n'])
# spot-check Northdata data still present on reset rows
nd=c.execute(\"SELECT COUNT(*) FROM companies WHERE stage='pending_ai' AND northdata_raw IS NOT NULL AND northdata_raw!=''\").fetchone()[0]
print('pending_ai rows that still have northdata_raw:', nd)
" 2>&1 | grep -v "^warning:"
```
Expected: `pending_ai` count jumped by ~114; those rows still have `northdata_raw`.

- [ ] **Step 4 (deferred): Re-run the pipeline for cologne**

This costs Claude usage and depends on a WORKING Northdata account (Task 1/2 now
make a broken account fail loudly). Run when ready — NOT part of the code change:
```bash
uv run python pipeline.py --input "input/2605 cologne hvac_master_list_with_categories.xlsx" --layout cologne_hvac 2>&1 | tee /tmp/cologne_rerun.log
```
Then confirm enrichment + no empty passthroughs:
```bash
grep -E "Stage 5|ai_enrich complete|Empty AI response|login FAILED" /tmp/cologne_rerun.log
```
Expected: Stage 5 re-enriches the parked companies; any still-empty ones are now
flagged (not silently blank); if the Northdata account is still cancelled, you
now see a loud `login FAILED` instead of silent degraded data.

---

## Self-Review

**Spec coverage:**
- Part 0 Northdata login-fail: Task 1 (raise) + Task 2 (catch, Stage-2-only). ✓
- Part A raw return + salvage: Task 3. ✓
- Part B Stage 5 store-raw + flag-empty: Task 4. ✓
- Part C cache write-guard: Task 5. ✓
- Non-destructive cologne recovery: Task 6 (no cache delete; preview-first). ✓

**Placeholder scan:** no TBD/TODO; every code step shows full code; every verify step has a concrete command + expected output. ✓

**Type/name consistency:** `NorthdataLoginError` defined in Task 1, imported/caught in Task 2. `enrich_company` return adds `"raw"` in Task 3, consumed in Task 4. `_is_enriched` defined + used in Task 5. `reset_for_rerun.py` invoked in Task 6 matches its real CLI (`db_paths` positional, `--dry-run`). ✓

**No-test-suite honored:** no pytest, no test files; `py_compile` + import + stubbed `python -c` only. ✓

**Double-stop hazard (flagged in spec):** resolved in Task 2 Step 2 — `start()` moved into its own try with an early `return` after `stop()`, so the loop's `finally: stop()` is never reached on the login-failure path. ✓