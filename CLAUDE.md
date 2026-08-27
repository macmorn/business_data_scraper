# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

EU Company Research Pipeline — a resumable, multi-stage pipeline that extracts company names from PDF/Excel supplier lists, enriches them through multiple data sources (Northdata, OpenCorporates, GLEIF, Pappers, Brave Search, Claude AI), and exports enriched CSV files.

## Commands

```bash
# Install dependencies (uses uv, not pip)
uv sync
uv run playwright install chromium

# Run the full pipeline
uv run python pipeline.py

# Common CLI options
uv run python pipeline.py --input suppliers.pdf --layout airbus_suppliers
uv run python pipeline.py --countries DE,FR,GB
uv run python pipeline.py --fresh          # delete DB and start over
uv run python pipeline.py --list-layouts   # show available PDF layouts
uv run python pipeline.py --seed-cache data/previous.db  # seed cache from prior run

# Entry point is also registered as a script
uv run run-pipeline
```

There is no test suite. No linter configuration.

## Architecture

### Pipeline Stages (sequential, resumable via SQLite)

Each company record has a `stage` field in the DB. Re-running the pipeline picks up from where it left off — stages skip companies that have already advanced past them.

```
PDF/Excel → S01 extract → S02 northdata → S03 registry fallback
→ S04 ceo lookup → S04b structure → S05 ai enrich
→ S06 normalize → S07 export → CSV
```

| Stage | File | Async | Purpose |
|-------|------|-------|---------|
| S01 | `stages/s01_pdf_extract.py` | no | Parse PDF/Excel, filter by country, route to northdata or fallback |
| S02 | `stages/s02_northdata.py` | yes | Headless Playwright browser scraping with stealth (primary source) |
| S03 | `stages/s03_registry_fallback.py` | yes | OpenCorporates → GLEIF → Pappers → Brave (first match wins) |
| S04 | `stages/s04_ceo_lookup.py` | yes | Extract CEO from officers JSON via role priority matching |
| S04b | `stages/s04b_structure.py` | yes | Follow holding/KG corporate structure links |
| S05 | `stages/s05_ai_enrich.py` | yes | Claude AI disambiguation + CEO career summaries |
| S06 | `stages/s06_normalize.py` | no | Standardize legal forms, employee ranges, confidence scoring |
| S07 | `stages/s07_export.py` | no | UTF-8 BOM CSV + separate review CSV for flagged entries |

### Key Modules

- **`pipeline.py`** — Orchestrator, CLI argument parsing, stage sequencing
- **`models.py`** — `CompanyRecord` dataclass (45+ fields) and `CSV_COLUMNS` output schema; stage constants (`STAGE_PENDING_NORTHDATA`, etc.)
- **`db.py`** — SQLite WAL-mode persistence, queue ops (`get_pending`, `update_company`, `mark_failed`), resumability
- **`config.py`** — All settings from `.env` via `python-dotenv`, no hardcoded secrets
- **`cache.py`** — Cross-run enrichment cache (separate SQLite DB) to skip stages 2-6 for known companies
- **`pdf_layouts.py`** — Pluggable format profiles for different PDF structures (field mapping, regex, dedup)

### Clients (data source integrations, in `clients/`)

- **`northdata_browser.py`** — Playwright stealth automation (rate-limited 2.5-8s between requests)
- **`opencorporates.py`**, **`gleif.py`**, **`pappers.py`**, **`brave_search.py`** — httpx async clients
- **`claude_ai.py`** — Uses `claude-agent-sdk` (not the Anthropic API directly) with web search tool
- **`website_scraper.py`** — BeautifulSoup fallback for CEO extraction from company websites

### Utilities (in `utils/`)

- **`rate_limiter.py`** — Async token-bucket rate limiter
- **`retry.py`** — Exponential backoff decorator with jitter
- **`logging_setup.py`** — Dual stdout + file logging with ETA tracking

## Key Patterns

- **Async stages** use `asyncio.Semaphore` for concurrency control; sync stages run sequentially
- **Resumability**: raw API responses cached in DB columns (`northdata_raw`, `opencorporates_raw`, etc.) so retries skip HTTP calls
- **Stage transitions**: each stage reads companies at its `pending_*` stage, processes them, then advances to the next stage via `db.update_company()`
- **Error handling**: `db.mark_failed()` increments `retry_count`; companies exceeding `MAX_RETRIES` move to `STAGE_FAILED`
- **Country routing**: S01 splits companies into Northdata-covered countries (→ S02) vs others (→ S03 fallback)

## Configuration

All config via `.env` (see `.env.example`). Key variables:
- `BRAVE_API_KEY`, `PAPPERS_API_KEY`, `OPENCORPORATES_API_KEY` — API keys
- `NORTHDATA_EMAIL`, `NORTHDATA_PASSWORD` — Northdata login credentials
- `INPUT_PDF`, `OUTPUT_CSV`, `DB_PATH` — file paths (auto-derived from input filename if not set)
- `PDF_LAYOUT` — which layout profile to use for parsing
- `NORTHDATA_DELAY_MIN/MAX` — rate limiting for browser automation
- `MAX_RETRIES` — per-stage retry limit
