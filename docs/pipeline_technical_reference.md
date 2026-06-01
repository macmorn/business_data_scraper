# EU Company Research Pipeline — Technical Reference

## Table of Contents

1. [Pipeline Overview](#1-pipeline-overview)
2. [Stage 1: PDF Extraction](#2-stage-1-pdf-extraction)
3. [Stage 2: Northdata Lookup](#3-stage-2-northdata-lookup)
4. [Stage 3: Registry Fallback](#4-stage-3-registry-fallback)
5. [Stage 4: CEO Identification](#5-stage-4-ceo-identification)
6. [Stage 5: AI Enrichment](#6-stage-5-ai-enrichment)
7. [Stage 6: Normalization](#7-stage-6-normalization)
8. [Stage 7: CSV Export](#8-stage-7-csv-export)
9. [Supporting Infrastructure](#9-supporting-infrastructure)
10. [Data Model](#10-data-model)

---

## 1. Pipeline Overview

### Purpose

This pipeline takes a PDF supplier list (e.g., Airbus Approved Suppliers), extracts company names, and enriches each company with registry data, financial information, leadership details, and AI-generated summaries. The final output is a Google Sheets-compatible CSV.

### Architecture

```
PDF Input
    │
    ▼
┌──────────────────┐
│  Stage 1: PDF    │  Extract & deduplicate company names, filter by country
│  Extraction      │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Stage 2:        │  Headless browser scraping of northdata.com
│  Northdata       │──── found ────────────────────────────────┐
│  Lookup          │──── multiple matches ── Stage 5 (AI) ──┐  │
└────────┬─────────┘                                        │  │
         │ not found                                        │  │
         ▼                                                  │  │
┌──────────────────┐                                        │  │
│  Stage 3:        │  OpenCorporates → GLEIF → Pappers →    │  │
│  Registry        │  Brave Search (first match wins)       │  │
│  Fallback        │                                        │  │
└────────┬─────────┘                                        │  │
         │                                                  │  │
         ▼                                                  │  │
┌──────────────────┐                                        │  │
│  Stage 4: CEO    │  Extract CEO from officers JSON ◄──────┼──┘
│  Identification  │  using role-based priority scoring     │
└────────┬─────────┘                                        │
         │                                                  │
         ▼                                                  │
┌──────────────────┐                                        │
│  Stage 5: AI     │  Claude: disambiguate + career ◄───────┘
│  Enrichment      │  summaries
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Stage 6:        │  Legal form abbreviation, employee
│  Normalization   │  bucketing, confidence scoring
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Stage 7: CSV    │  UTF-8 BOM CSV + review CSV
│  Export          │
└──────────────────┘
```

### Stage Progression

Each company record carries a `stage` field in the SQLite database that determines which stage processes it next:

```
new → pending_northdata → pending_ceo → pending_ai → pending_normalize → pending_export → done
                        ↘ pending_fallback → pending_ceo ↗
                        ↘ pending_ai (multiple matches) ↗
                        ↘ failed (max retries exceeded)
```

The pipeline is **fully resumable** — on restart, each stage picks up only the companies at its corresponding stage, so no work is repeated.

### Orchestration (`pipeline.py`)

The main entry point runs all 7 stages sequentially:

1. **Stage 1** only runs if the database is empty (first run or `--fresh`)
2. **Stages 2–5** are `async` (use `asyncio` for I/O-bound operations)
3. **Stages 6–7** are synchronous (CPU-bound normalization + file I/O)

CLI arguments allow overriding the input PDF (`--input`), output path (`--output`), PDF layout (`--layout`), country filter (`--countries`), and database path (`--db`). The `--fresh` flag deletes the existing database for a clean start. The `--list-layouts` flag prints available PDF parsing layouts.

---

## 2. Stage 1: PDF Extraction

**File:** `stages/s01_pdf_extract.py`
**Input:** PDF file (path from `config.INPUT_PDF`)
**Output:** Company records in database at stage `pending_northdata`
**Sync/Async:** Synchronous

### Processing Flow

```
extract_text() → clean_lines() → parse_records() → deduplicate_records()
    → apply_field_mapping() → db.load_companies() → _apply_extra_data()
    → _route_by_region()
```

### Step 1: Text Extraction

Two backends are tried in order:

1. **`pdftotext`** (Poppler CLI tool) — the primary backend. Invoked as a subprocess with `pdftotext <file> -` which writes to stdout. This is used because many supplier-list PDFs use custom font encodings that corrupt text when processed by Python PDF libraries.
2. **PyMuPDF** (`fitz`) — fallback if pdftotext is unavailable or fails. Opens the PDF and concatenates text from all pages.

If pdftotext fails with a runtime error (not file-not-found), the fallback is tried. If the file doesn't exist, the error is raised immediately.

### Step 2: Line Cleaning

The raw text is split by newline. Each line is stripped and filtered:

- **Empty lines** are removed
- **Substring matches** against `layout.skip_patterns` (e.g., "AIRBUS APPROVAL", "Disclaimer:", copyright notices) — if any pattern appears anywhere in the line, the line is dropped
- **Exact matches** against `layout.skip_exact` (e.g., column headers like "Street", "City", "Country")
- **Standalone page numbers** (1–2 digit numbers on their own line, matched by regex `^\d{1,2}$`)

### Step 3: Record Parsing

The parser uses the layout's field definitions to identify repeating record blocks:

1. A compiled regex from `layout.record_start_pattern` identifies the start of each record (e.g., `^\d{6}$` for 6-digit vendor codes in the Airbus layout)
2. Starting from each matched line, the parser consumes `N` consecutive lines where `N` is the number of fields defined in the layout
3. Each line is assigned to its corresponding field by position
4. If fewer than `N` lines remain, the partial record is discarded

For the **Airbus layout**, each record is a 7-line block:
```
vendor_code    (6-digit, e.g. "100234")
company_name   (e.g. "Müller GmbH")
cage_code      (e.g. "S1234" or "#" for unknown)
street         (e.g. "Industriestr. 10")
city           (e.g. "40213 Düsseldorf")
country        (e.g. "Germany")
product_group  (e.g. "Raw Materials")
```

For the **simple_name_list** layout, each record is a single line (any line with 3+ characters).

### Step 4: Deduplication

If `layout.has_duplicate_rows` is `True`, records are deduplicated by company name (case-insensitive). The `dedup_collect_field` (e.g., `product_group`) accumulates unique values from duplicate entries into a semicolon-separated string.

Example: If "Müller GmbH" appears 3 times with product groups "Raw Materials", "Fasteners", and "Raw Materials", the result is a single record with `product_group = "Raw Materials; Fasteners"`.

### Step 5: Field Mapping

Layout-specific field names are renamed to `CompanyRecord` field names using `layout.field_mapping`. For the Airbus layout:
- `company_name` → `name_original`
- `street` → `address_street`
- `city` → `address_city`
- `country` → `address_country`

Unmapped fields retain their original names.

### Step 6: Database Loading

Company names are inserted into the SQLite database via `db.load_companies()`. Duplicates (by `name_original` UNIQUE constraint) are silently skipped. Extra data from the PDF (address, country) is written back to the database via `_apply_extra_data()`.

### Step 7: Country Routing

`_route_by_region()` filters companies based on country:

- **Included countries** (those in the `NORTHDATA_COUNTRIES` set of 28 EU codes, or a custom `--countries` filter): stage advanced to `pending_northdata`
- **Unknown country** (no country data from PDF): kept and advanced to `pending_northdata` (benefit of the doubt)
- **Excluded countries** (outside the filter): **deleted from the database entirely**

Country values from the PDF are normalized from full names (e.g., "Germany", "Deutschland") to ISO 3166-1 alpha-2 codes using a hardcoded mapping of 30+ name variants.

---

## 3. Stage 2: Northdata Lookup

**File:** `stages/s02_northdata.py`
**Client:** `clients/northdata_browser.py`
**Input:** Companies at stage `pending_northdata`
**Output:** Companies advanced to `pending_ceo`, `pending_ai`, `pending_fallback`, or `failed`
**Sync/Async:** Async

### Processing Flow

For each company in the batch:

1. Wait for rate limiter (3–8 second randomized delay)
2. Call `client.search(company.name_original)`
3. Route based on result status:
   - `found` (single match) → apply data, advance to `pending_ceo`
   - `multiple` (2+ matches) → store matches, flag for review, advance to `pending_ai`
   - `not_found` → advance to `pending_fallback`
   - `error` → call `db.mark_failed()`, increment retry counter

### Browser Client Architecture

The `NorthdataClient` class manages a Playwright headless Chromium browser with anti-detection measures:

**Startup sequence:**
1. Launch headless Chromium with `--disable-blink-features=AutomationControlled` and `--no-sandbox`
2. Apply `playwright-stealth` plugin to the browser context (patches `navigator.webdriver`, canvas fingerprinting, etc.)
3. Navigate to homepage and dismiss cookie consent dialog ("Accept all" button)
4. Optionally log in with premium credentials (email/password typed with randomized key delays)

**Search sequence (`_search_once`):**
1. Navigate to `https://www.northdata.com/`
2. Wait 800–2000ms (human reading delay)
3. Move mouse to random viewport position with 8–25 step curves
4. Check for Cloudflare challenge page (`challenge-platform` or `Just a moment` in HTML)
5. 40% chance: scroll the page randomly before interacting
6. Locate `input[name="query"]` search field
7. Move mouse toward the input box with realistic cursor movement
8. Click and clear the input, then type the company name with 25–75ms per-keystroke delay
9. Wait 2000–3500ms for AJAX autocomplete response
10. Parse autocomplete dropdown results

**Autocomplete parsing (`_parse_autocomplete`):**
- Scans all `<a>` tags on the page (up to 20)
- Filters out non-company links (help pages, privacy, terms, nav links) using the `_SKIP_HREFS` set
- Validates links contain URL-encoded comma, space, or plus signs (company page URL pattern)
- Extracts company name from first line of link text, removing dagger/cross symbols (†/✞ used for dissolved companies)
- Returns list of `{name, url, details}` dicts

**Result routing:**
- **0 results** → `not_found`
- **1 result** → follow link, scrape company page, return `found`
- **2+ results** → check for exact name match (case-insensitive). If found, follow that link. Otherwise return `multiple` with all candidates

**Retry logic:** The `search()` method retries up to `_retry_attempts` times (default 2) for `not_found` or `error` results. `found` and `multiple` results are returned immediately.

### Company Page Scraping

When a single match is identified, the browser navigates to the company's Northdata page and extracts structured data:

**Header parsing:**
- Company name from `<h1>` tag
- Legal form extracted from the company name using suffix matching against a list of 30+ forms (GmbH, AG, SAS, B.V., Ltd, etc.)

**Body text regex extraction:**
- **Status:** Keywords `gelöscht/dissolved/liquidat/insolv` → "dissolved"; `aktiv/active/registered` → "active"
- **Address:** Regex match for content between `ADDRESS\n` and the next section header
- **Corporate purpose:** Regex match for content between `CORPORATE PURPOSE\n` and the next section, with industry code extracted from the first line
- **Founded year:** Regex for `gegründet/founded/incorporated/Registration` followed by a 4-digit year

**Identification section parsing (`_parse_identification`):**
- Finds the `IDENTIFICATION` section in body text
- Parses line-by-line looking for label/value pairs:
  - `Ut` label → next line is register court (e.g., "District Court of Neuss HRB 1878"), with register ID extracted via regex
  - `Lei` label → next line is LEI number
  - `VAT Id` label → next line is VAT identification number

**Representatives table parsing (`_parse_representatives_table`):**
- Locates `<table class="company-representatives">`
- Iterates rows, tracking current position/role from the first `<td>`
- Person name, location, and optional Northdata profile URL extracted from the second `<td>`
- Person text split by comma: first part = name, remainder = location
- Returns list of `{role, name, location, northdata_url}` dicts

**Financials KPI table parsing (`_parse_financials_table`):**
- Locates `<table class="bizq">` with "Financials" or "Finanzkennzahlen" in header
- Parses date columns from header row (each column = one fiscal year)
- Maps 30+ row labels (English and German) to standardized keys using exact prefix matching:
  - Revenue, Earnings, Total assets, Equity, Equity ratio, Employee number, Return on sales, Cost of materials, Wages and salaries, Cash on hand, Liabilities, Pension provisions, Auditor, etc.
  - Explicitly skips derived metrics: Revenue CAGR, Earnings CAGR, Revenue per employee, Average salaries per employee
- Values are cleaned by removing annotations (`* via LLM`, `German HGB ...`, year suffixes)
- Most recent column values are set on the data dict; if the most recent column is N/A, falls back to the next most recent column
- Full history (all years × all metrics) stored as JSON

**Marketing table parsing (`_parse_mktg_table`):**
- Locates second `.bizq` table with "Mktg" or "Marketing" header
- Extracts "total public funding" / "Gesamtförderung" value from the most recent non-N/A column

### Data Applied to CompanyRecord

After scraping, `_apply_company_data()` maps all extracted fields to the `CompanyRecord`:
- Basic: matched_name, legal_form, status, address, founded_year
- Identification: register_id, register_court, lei, vat_id
- Financials: revenue, earnings, total_assets, equity, equity_ratio, employees_count, return_on_sales, cost_of_materials, wages_and_salaries, cash_on_hand, liabilities, pension_provisions, auditor, financials_json, public_funding_total
- Corporate: corporate_purpose, industry_code
- Officers: JSON array of officer dicts
- Country is guessed from the address string using keyword matching (Deutschland→DE, etc.)
- Data source is recorded as "northdata"

---

## 4. Stage 3: Registry Fallback

**File:** `stages/s03_registry_fallback.py`
**Input:** Companies at stage `pending_fallback` (not found on Northdata)
**Output:** Companies advanced to `pending_ceo`
**Sync/Async:** Async

### Fallback Chain

Each company is tried against 4 data sources in strict priority order. The chain **stops on the first source that returns a result with a company name**:

```
1. OpenCorporates API  →  if data.name exists → stop
2. GLEIF LEI Registry  →  if data.name exists → stop
3. Pappers.fr          →  if data.name exists → stop  (only if country is FR or unknown)
4. Brave Search        →  if data.country or data.founded_year exists → stop
```

If no source returns usable data, the company is flagged for manual review (`needs_review_flag = True`).

All companies advance to `pending_ceo` regardless of whether fallback data was found.

### Source 1: OpenCorporates (`clients/opencorporates.py`)

- **API:** `https://api.opencorporates.com/v0.4/companies/search`
- **Auth:** Optional API key (free tier works with rate limits)
- **Method:** `search_company_eu()` searches without jurisdiction filter, verifies result is from an EU jurisdiction
- **Data extracted:** name, country (from jurisdiction code, uppercased first 2 chars), legal form (company_type), status (normalized: active/dissolved), founded year (from incorporation_date), address, officers list, OpenCorporates URL
- **Status normalization:** "active/live/registered/good standing" → "active"; "dissolved/closed/struck off/liquidat" → "dissolved"

### Source 2: GLEIF (`clients/gleif.py`)

- **API:** `https://api.gleif.org/api/v1/fuzzycompletions`
- **Auth:** None required (free public API)
- **Method:** Fuzzy name search on `entity.legalName` field. Fallback: `search_company_full()` tries exact legal name filter, then fulltext search
- **Data extracted:** name, country (from legal address), legal form (from entity legalForm id), status (ACTIVE→"active", INACTIVE/DISSOLVED→"dissolved"), address (built from addressLines + city + region + postalCode + country), LEI number

### Source 3: Pappers (`clients/pappers.py`)

- **API:** `https://api.pappers.fr/v2/recherche`
- **Auth:** Required (`PAPPERS_API_KEY`)
- **Scope:** French companies only — skipped if company country is known and not "FR"
- **Data extracted:** name (nom_entreprise), country (always "FR"), legal form (forme_juridique), status ("Inscrit" → "active"), founded year (from date_creation), address (from siege fields: adresse_ligne_1, adresse_ligne_2, code_postal, ville), officers (from representants: prenom + nom, qualite as role), employee range (tranche_effectif), revenue range (formatted: Mrd/Mio/k EUR), SIREN number
- **Revenue formatting:** ≥1B → "X.X Mrd EUR", ≥1M → "X.X Mio EUR", ≥1K → "Xk EUR"

### Source 4: Brave Search (`clients/brave_search.py`)

- **API:** `https://api.search.brave.com/res/v1/web/search`
- **Auth:** Required (`BRAVE_API_KEY`)
- **Method:** Web search for `"<company name>" company registry`, returns top 10 results
- **Extraction strategy:** Concatenates all result titles and descriptions into a single text blob, then applies regex extraction:
  - **Country:** Matches against 9 country keyword patterns (Germany/Deutschland → DE, France/French → FR, etc.)
  - **Founded year:** Regex `founded/established/gegründet/incorporated <YYYY>`, validated range 1800–2026
  - **Status:** "active/operating/in business" → "active"; "dissolved/closed/liquidat/bankrupt/insolvent" → "dissolved"
  - **Address:** Regex for text following "address/headquarter/HQ/Sitz"

### Data Application

`_apply_fallback_data()` fills **only empty fields** — it never overwrites data already present on the company record. This preserves any partial data from earlier stages. The raw API response is stored in the corresponding `*_raw` column, and the source name is appended to `data_sources_used`.

---

## 5. Stage 4: CEO Identification

**File:** `stages/s04_ceo_lookup.py`
**Input:** Companies at stage `pending_ceo`
**Output:** Companies advanced to `pending_ai`
**Sync/Async:** Async

### Logic

The CEO is extracted from the `officers` JSON field (populated by Northdata or registry fallback sources). No external API calls are made in this stage.

### Role Priority Scoring

The `CEO_ROLES` list defines executive role keywords in descending priority:

| Priority | Role Keywords |
|----------|---------------|
| Highest  | geschäftsführer, geschäftsfuhrer, geschaeftsfuehrer |
| High     | ceo, chief executive officer |
| Medium   | managing director, directeur général/general |
| Medium   | gérant/gerant, président/president |
| Lower    | vorstandsvorsitzender, vorstand |
| Lower    | amministratore delegato, director general |
| Lowest   | bestuurder, zaakvoerder |

### Scoring Algorithm

```python
for each officer in officers_json:
    role_lower = officer.role.lower()
    for i, ceo_role in enumerate(CEO_ROLES):
        if ceo_role in role_lower:
            score = len(CEO_ROLES) - i    # higher priority = higher score
            break
    track best scoring officer
```

The officer with the highest score is selected as the CEO. If no officer's role matches any keyword (score remains 0), no CEO is identified.

### Output Fields

- `ceo_name`: Full name of the identified CEO
- `ceo_current_title`: The officer's role string (as-is from the data source)
- `ceo_confidence`: "high" if found, "not found" if no matching officer

If no CEO is found, the company is flagged for manual review (`needs_review_flag = True`).

---

## 6. Stage 5: AI Enrichment

**File:** `stages/s05_ai_enrich.py`
**Client:** `clients/claude_ai.py`
**Input:** Companies at stage `pending_ai`
**Output:** Companies advanced to `pending_normalize`
**Sync/Async:** Async

### Two AI Tasks

Each company may trigger up to two Claude calls:

#### Task 1: Disambiguation

**Triggered when:** `northdata_raw` contains a result with `status == "multiple"` and a non-empty `matches` list.

**Process:**
1. Build context hints from known company data (country, address)
2. Format all candidates as a numbered list with name and details
3. Send prompt to Claude with JSON schema output format requiring `{index, confidence, reasoning}`
4. Apply the chosen match:
   - If `index` is valid (0-based, within range): set `matched_name` and `confidence_score`
   - If `confidence < 0.7`: flag for manual review
   - If `index == -1` (no good match): flag for review, set confidence to 0.0

**Prompt structure:**
```
Given the company name "<original>", which of these search results is the best match?
Consider name similarity, country, active status, and address plausibility.
Additional context: {country_hint, address_hint}

Candidates:
0. Name A (details)
1. Name B (details)
...

If none are a good match, use index: -1.
```

#### Task 2: Career Summary

**Triggered when:** `ceo_name` and `ceo_current_title` are both populated.

**Process:**
1. Send prompt requesting a 2–3 sentence professional career summary
2. Prompt focuses on: career trajectory, notable companies, domain expertise
3. Result stored in `ceo_career_summary`
4. Errors are caught and logged but don't block the pipeline

**Prompt structure:**
```
Write a 2-3 sentence professional career summary for <name>,
currently <title> at <company>.
Focus on: career trajectory, notable companies, domain expertise.
Write in third person. Be concise and factual.
```

### Claude Client Architecture (`clients/claude_ai.py`)

- **SDK:** Uses Claude Agent SDK (runs through Claude Code/Max plan, no per-token billing)
- **Concurrency:** `asyncio.Semaphore(5)` limits concurrent Claude calls
- **Retry:** `@with_retry(max_attempts=3, base_delay=2.0)` with exponential backoff for `CLIConnectionError` and `ProcessError`
- **Configuration:** `max_turns=1`, `allowed_tools=[]` (no tool use), optional `system_prompt` and `output_format` (JSON schema)

The `_ask_claude()` function streams the response and extracts the final `ResultMessage.result` text. For disambiguation, the result is parsed as JSON; for career summaries, the raw text is used directly.

An additional function `extract_ceo_from_text()` exists for extracting CEO info from unstructured text via Claude, but is not currently integrated into the pipeline.

---

## 7. Stage 6: Normalization

**File:** `stages/s06_normalize.py`
**Input:** Companies at stage `pending_normalize`
**Output:** Companies advanced to `pending_export`
**Sync/Async:** Synchronous

### Three Normalization Steps

#### 1. Legal Form Abbreviation

Full legal form names are mapped to standard abbreviations using substring matching (case-insensitive):

| Full Name | Abbreviation |
|-----------|-------------|
| Gesellschaft mit beschränkter Haftung | GmbH |
| Aktiengesellschaft | AG |
| Kommanditgesellschaft | KG |
| Société par actions simplifiée | SAS |
| Société à responsabilité limitée | SARL |
| Société anonyme | SA |
| Besloten vennootschap | B.V. |
| Naamloze vennootschap | N.V. |
| Società per azioni | S.p.A. |
| Società a responsabilità limitata | S.r.l. |
| Sociedad limitada | S.L. |
| Public limited company | PLC |
| Limited / Private limited | Ltd |
| *...and 8 more* | |

If no mapping matches, the original legal form string is kept as-is.

#### 2. Employee Range Bucketing

Raw employee count strings are normalized into standard range buckets:

1. Extract all number sequences from the string (digits and commas, periods removed)
2. Take the **largest** number found
3. Map to the appropriate bucket:

| Range | Label |
|-------|-------|
| 1–10 | "1-10" |
| 11–50 | "11-50" |
| 51–200 | "51-200" |
| 201–500 | "201-500" |
| 501–1000 | "501-1000" |
| 1001+ | "1000+" |

If no numbers can be extracted, the original string is kept.

#### 3. Confidence Score Computation

A weighted completeness score (0.0–1.0) is computed based on which fields are populated:

| Field | Weight | Condition |
|-------|--------|-----------|
| matched_name | 0.20 | Must differ from name_original |
| country | 0.15 | Not None |
| legal_form | 0.10 | Not None |
| status | 0.10 | Not None |
| founded_year | 0.10 | Not None |
| address | 0.10 | Not None |
| officers | 0.10 | Not None |
| ceo_name | 0.10 | Not None |
| data_sources_used | 0.05 | Not None |

**Formula:** `score = sum(weight for populated fields) / sum(all weights)`

The confidence score is only computed if not already set (e.g., by disambiguation). If the score is below **0.5**, the company is flagged for manual review.

Additionally, if `matched_name` is empty, it defaults to `name_original`.

---

## 8. Stage 7: CSV Export

**File:** `stages/s07_export.py`
**Input:** Companies at stage `pending_export`
**Output:** Two CSV files + companies marked as `done`
**Sync/Async:** Synchronous

### Output Files

**1. Main CSV** (`output/companies_enriched.csv`):
- Contains all companies
- Encoded as **UTF-8 with BOM** (`utf-8-sig`) for Google Sheets compatibility
- Column order defined by `models.CSV_COLUMNS` (38 columns)
- Built using pandas DataFrame

**2. Review CSV** (`output/needs_review.csv`):
- Subset of main CSV filtered to `needs_review_flag == True`
- Same format and encoding
- Only created if there are flagged companies

### Column Order

```
company_name_original, matched_name, country, legal_form, status, address,
register_id, register_court, lei, vat_id, industry_code, corporate_purpose,
founded_year, employees_count, revenue, earnings, total_assets, equity,
equity_ratio, return_on_sales, cost_of_materials, wages_and_salaries,
cash_on_hand, liabilities, pension_provisions, auditor, public_funding_total,
last_accounts_year, officers, ceo_name, ceo_linkedin_url, ceo_current_title,
ceo_career_summary, ceo_confidence, northdata_url, data_sources_used,
confidence_score, needs_review_flag, financials_json
```

After export, all companies are marked as `stage = "done"`.

---

## 9. Supporting Infrastructure

### Database Layer (`db.py`)

**Engine:** SQLite with WAL (Write-Ahead Logging) mode for crash resilience.

**Schema:** Single `companies` table with 60+ columns, auto-incremented primary key, `name_original UNIQUE` constraint, and automatic `created_at`/`updated_at` timestamps.

**Indexes:**
- `idx_stage` on `stage` — fast lookup for batch processing
- `idx_stage_retry` on `(stage, retry_count)` — efficient filtering of exhausted retries

**Connection settings:**
- WAL journal mode (`PRAGMA journal_mode=WAL`)
- 5-second busy timeout (`PRAGMA busy_timeout=5000`)
- `Row` factory for dict-style column access

**Key operations:**
- `load_companies(names)` — INSERT OR IGNORE for idempotent loading
- `get_pending(stage, limit)` — fetches companies at a given stage with `retry_count < MAX_RETRIES`
- `update_company(record)` — full UPDATE of all 45+ fields by primary key
- `mark_failed(record_id, error)` — increments retry_count, moves to `failed` stage when retries exhausted
- `get_stats()` — GROUP BY stage count for progress reporting

### Rate Limiter (`utils/rate_limiter.py`)

Async token-bucket rate limiter with randomized jitter:

- Constructor takes `min_delay` and `max_delay` (seconds)
- `wait()` computes a random delay between min and max, then sleeps for the remaining time since the last call
- Uses `asyncio.Lock` to serialize concurrent callers
- Used by Stage 2 with 3.0–8.0 second delays

### Retry Decorator (`utils/retry.py`)

`@with_retry(max_attempts, base_delay, exceptions)` — async decorator implementing exponential backoff:

- **Delay formula:** `base_delay × 2^attempt + random(0, 1)` seconds
- Only catches specified exception types; others propagate immediately
- Logs each retry attempt with function name, attempt count, error, and delay
- Applied to all API client functions and Claude SDK calls

### Logging (`utils/logging_setup.py`)

**Dual output:** stdout + file (`output/pipeline.log`)
**Format:** `timestamp | LEVEL   | logger_name               | message`
**Noisy libraries suppressed:** httpx, httpcore, playwright set to WARNING level

**ProgressTracker class:**
- Initialized with total count and stage name
- `tick(company_name, result)` — logs per-company progress as `[N/total] name -> result (ETA: XmYYs)`
- ETA calculated from `(total - processed) / (processed / elapsed_seconds)`
- `summary(results)` — logs stage completion with elapsed time and result breakdown

### Configuration (`config.py`)

All configuration is loaded from `.env` via `python-dotenv`:

| Variable | Default | Description |
|----------|---------|-------------|
| `BRAVE_API_KEY` | "" | Brave Search API key |
| `PAPPERS_API_KEY` | "" | Pappers.fr API key |
| `OPENCORPORATES_API_KEY` | "" | OpenCorporates API key (optional, free tier works) |
| `NORTHDATA_DELAY_MIN` | 3.0 | Minimum seconds between Northdata requests |
| `NORTHDATA_DELAY_MAX` | 8.0 | Maximum seconds between Northdata requests |
| `NORTHDATA_EMAIL` | "" | Premium Northdata login email |
| `NORTHDATA_PASSWORD` | "" | Premium Northdata login password |
| `NORTHDATA_RETRY_ATTEMPTS` | 2 | Retries per company on Northdata |
| `INPUT_PDF` | "input/companies.pdf" | Path to source PDF |
| `OUTPUT_CSV` | "output/companies_enriched.csv" | Path to output CSV |
| `DB_PATH` | "data/pipeline.db" | Path to SQLite database |
| `LOG_FILE` | "output/pipeline.log" | Path to log file |
| `PDF_LAYOUT` | "airbus_suppliers" | Active PDF parsing layout |
| `MAX_RETRIES` | 3 | Max retry attempts before marking a company as failed |

---

## 10. Data Model

### CompanyRecord (`models.py`)

The central dataclass that represents a company throughout the pipeline:

```
Identification:        id, name_original, matched_name
Company info:          country, legal_form, status, founded_year, address
Registration:          register_id, register_court, lei, vat_id
Financials:            revenue, earnings, total_assets, equity, equity_ratio,
                       employees_count, return_on_sales, cost_of_materials,
                       wages_and_salaries, cash_on_hand, liabilities,
                       pension_provisions, auditor, financials_json,
                       public_funding_total
Corporate:             corporate_purpose, industry_code, employees_range,
                       revenue_range, last_accounts_year, officers (JSON)
Leadership:            ceo_name, ceo_linkedin_url, ceo_current_title,
                       ceo_career_summary, ceo_confidence
Metadata:              data_sources_used, confidence_score, needs_review_flag,
                       northdata_url
Pipeline control:      stage, error, retry_count
Raw response cache:    northdata_raw, opencorporates_raw, gleif_raw,
                       pappers_raw, brave_raw
```

### PDF Layout System (`pdf_layouts.py`)

Two dataclasses define the parsing configuration:

**`FieldDef`:** `name` (string), `position` (0-based int), `is_key` (bool — identifies record start)

**`PDFLayout`:** `name`, `description`, `fields` (list of FieldDef), `skip_patterns` (substring filters), `skip_exact` (exact line filters), `record_start_pattern` (regex), `has_duplicate_rows` (bool), `dedup_collect_field` (field to merge on dedup), `field_mapping` (layout name → CompanyRecord name)

**Built-in layouts:**
- `airbus_suppliers` — 7-field records starting with 6-digit vendor codes, deduplicates by company name collecting product groups
- `simple_name_list` — single-field records (any line with 3+ characters)

New layouts can be added by creating a `PDFLayout` instance and registering it in the `LAYOUTS` dict.
