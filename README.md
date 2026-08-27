# EU Company Research Pipeline

A one-off pipeline to research EU-based companies from a PDF source file. Extracts company names, enriches each record with structured data from multiple registry sources (Northdata, OpenCorporates, GLEIF, Pappers, Brave), identifies CEO/leadership, and outputs a Google Sheets-compatible CSV.

Designed to run locally overnight for ~700 companies with SQLite-based resumability.

## Quick Start

### Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/getting-started/installation/) (Python package manager)
- `poppler-utils` (for PDF text extraction)
- Chromium browser (installed via Playwright)

### 1. Install system dependencies

```bash
# Ubuntu/Debian
sudo apt-get install poppler-utils

# macOS
brew install poppler
```

### 2. Clone and set up the project

```bash
git clone <repo-url>
cd business_data_scraper

# Create virtual environment and install dependencies
uv sync

# Install Playwright's Chromium browser
uv run playwright install chromium
```

### 3. Configure environment

```bash
cp .env.example .env
```

Edit `.env` and fill in your API keys:

```bash
# Required for AI disambiguation + career summaries
ANTHROPIC_API_KEY=sk-ant-...

# Optional - improves coverage for companies not found on Northdata
BRAVE_API_KEY=            # https://brave.com/search/api/
PAPPERS_API_KEY=          # https://www.pappers.fr/api (French companies)
OPENCORPORATES_API_KEY=   # https://api.opencorporates.com/ (optional, works without key at lower rate)
```

### 4. Add your PDF

```bash
cp /path/to/your/suppliers.pdf input/companies.pdf
```

If your PDF uses a different format than the Airbus supplier list, set the layout in `.env`:

```bash
# Options: airbus_suppliers, simple_name_list
PDF_LAYOUT=airbus_suppliers
```

### 5. Run the pipeline

```bash
uv run python pipeline.py
```

#### CLI Flags

| Flag | Short | Description |
|------|-------|-------------|
| `--input PATH` | `-i` | Path to source PDF (overrides `INPUT_PDF` in `.env`) |
| `--output PATH` | `-o` | Path to output CSV (overrides `OUTPUT_CSV` in `.env`) |
| `--layout NAME` | `-l` | PDF parsing layout (e.g. `airbus_suppliers`, `simple_name_list`) |
| `--countries CODES` | `-c` | Comma-separated ISO country codes to include (e.g. `DE,FR,GB`). Only companies from these countries are kept. Default: all Northdata-covered EU countries |
| `--db PATH` | | Path to SQLite database (overrides `DB_PATH` in `.env`) |
| `--fresh` | | Delete existing database and start from scratch |
| `--list-layouts` | | List available PDF layouts and exit |

#### Examples

```bash
# Run with default settings
uv run python pipeline.py

# Only process German and French companies
uv run python pipeline.py --countries DE,FR

# Use a different PDF and layout
uv run python pipeline.py --input input/other_list.pdf --layout simple_name_list

# Wipe the database and re-run from scratch
uv run python pipeline.py --fresh

# See available PDF layouts
uv run python pipeline.py --list-layouts
```

Monitor progress in a second terminal:

```bash
tail -f output/pipeline.log
```

The pipeline is **resumable** — if interrupted, re-run the same command and it picks up where it left off.

### 6. Output

- `output/companies_enriched.csv` — main results (import directly into Google Sheets)
- `output/needs_review.csv` — companies flagged for manual review
- `output/pipeline.log` — full run log
- `data/pipeline.db` — SQLite database with all cached data

## Pipeline Stages

| # | Stage | What it does |
|---|-------|-------------|
| 1 | **PDF Extraction** | Parses company names from PDF via `pdftotext`, deduplicates, loads into SQLite queue |
| 2 | **Northdata Lookup** | Searches northdata.com via headless browser (Playwright + stealth), rate-limited at 2.5s/request |
| 3 | **Registry Fallback** | Companies not found on Northdata are tried against OpenCorporates → GLEIF → Pappers → Brave |
| 4 | **CEO Identification** | Extracts CEO/Geschäftsführer from officer data found in registries |
| 5 | **AI Enrichment** | Claude API resolves multi-match disambiguation and generates CEO career summaries |
| 6 | **Normalization** | Standardizes country codes, legal forms, employee ranges; computes confidence scores |
| 7 | **CSV Export** | Writes final CSV (UTF-8 BOM for Google Sheets) and a separate review file for flagged entries |

## Output Schema (CSV columns)

| Column | Description |
|--------|-------------|
| `company_name_original` | Name as extracted from PDF |
| `matched_name` | Name as matched in data source |
| `country` | ISO country code (DE, FR, etc.) |
| `legal_form` | e.g. GmbH, SAS, BV, Ltd |
| `status` | active / dissolved / unknown |
| `founded_year` | Year of incorporation |
| `address` | Registered address |
| `employees_range` | e.g. 51-200, 1000+ |
| `revenue_range` | Where available |
| `last_accounts_year` | Most recent filing year |
| `officers` | Directors / management (JSON) |
| `ceo_name` | Identified CEO or Geschäftsführer |
| `ceo_linkedin_url` | LinkedIn profile URL if found |
| `ceo_current_title` | Current role title |
| `ceo_career_summary` | AI-generated 2-3 sentence summary |
| `ceo_confidence` | high / medium / not found |
| `data_sources_used` | Which sources contributed data |
| `confidence_score` | 0-1 overall match quality |
| `needs_review_flag` | True if manual review recommended |

## PDF Layout System

The parser uses configurable layout profiles defined in `pdf_layouts.py`. Each profile describes:

- Field names and positions within a repeating record block
- A regex pattern identifying the start of each record
- Lines to skip (headers, footers, boilerplate)
- Deduplication rules (e.g. collect product groups when same company appears multiple times)
- Field name mappings to the internal data model

**Included layouts:**

- **`airbus_suppliers`** — Airbus Approved Suppliers List. 7-field records: vendor code, company name, CAGE code, street, city, country, product group. Pre-extracts address and country from the PDF.
- **`simple_name_list`** — Plain list of company names, one per line.

To add a new layout, create a `PDFLayout` instance in `pdf_layouts.py` and add it to the `LAYOUTS` dict.

> **Note:** The pipeline uses `pdftotext` (poppler-utils) rather than Python PDF libraries because many supplier-list PDFs use custom font encodings that produce garbled output with pdfplumber/PyMuPDF. PyMuPDF is included as a fallback for standard PDFs.

## Project Structure

```
business_data_scraper/
├── pipeline.py              # Main entry point — runs all stages in sequence
├── config.py                # Settings loaded from .env
├── models.py                # CompanyRecord dataclass + CSV column definitions
├── db.py                    # SQLite schema, queue operations, cache layer
├── pdf_layouts.py           # Configurable PDF format profiles
├── stages/
│   ├── s01_pdf_extract.py   # PDF → company names (pdftotext backend)
│   ├── s02_northdata.py     # Playwright headless browser lookup
│   ├── s03_registry_fallback.py  # OpenCorporates, GLEIF, Pappers, Brave
│   ├── s04_ceo_lookup.py    # CEO extraction from officer data
│   ├── s05_ai_enrich.py     # Claude AI: disambiguation + career summaries
│   ├── s06_normalize.py     # Field standardization + confidence scoring
│   └── s07_export.py        # SQLite → CSV export
├── clients/
│   ├── northdata_browser.py # Playwright + stealth wrapper
│   ├── opencorporates.py    # OpenCorporates API v0.4
│   ├── gleif.py             # GLEIF LEI lookup (free, no auth)
│   ├── pappers.py           # Pappers.fr API (French companies)
│   ├── brave_search.py      # Brave Search API (gap-filler)
│   ├── claude_ai.py         # Anthropic SDK wrapper
│   └── website_scraper.py   # Generic /team /about page scraper
├── utils/
│   ├── logging_setup.py     # Dual stdout + file logging with ETA
│   ├── rate_limiter.py      # Async token-bucket rate limiter
│   └── retry.py             # Exponential backoff decorator
├── input/                   # Drop source PDFs here
├── output/                  # CSV output + logs
└── data/                    # SQLite database (gitignored)
```

## Data Sources

| Priority | Source | Coverage | Auth Required |
|----------|--------|----------|---------------|
| 1 | Northdata (web scraping) | DE, AT, CH + expanding EU | None (headless browser) |
| 2 | OpenCorporates API | All EU countries | Optional (free tier available) |
| 3 | GLEIF API | Global LEI registry | None (free) |
| 4 | Pappers.fr API | France | API key required |
| 5 | Brave Search API | General web | API key required |

## Configuration Reference

All settings in `.env`:

| Variable | Default | Description |
|----------|---------|-------------|
| `ANTHROPIC_API_KEY` | — | **Required.** Claude API key for disambiguation + summaries |
| `BRAVE_API_KEY` | — | Brave Search API key |
| `PAPPERS_API_KEY` | — | Pappers.fr API key (French companies) |
| `OPENCORPORATES_API_KEY` | — | OpenCorporates API key (works without at lower rate) |
| `NORTHDATA_DELAY_SECONDS` | `2.5` | Minimum seconds between Northdata requests |
| `INPUT_PDF` | `input/companies.pdf` | Path to source PDF |
| `OUTPUT_CSV` | `output/companies_enriched.csv` | Path for main output CSV |
| `DB_PATH` | `data/pipeline.db` | SQLite database path |
| `PDF_LAYOUT` | `airbus_suppliers` | PDF layout profile name |
| `CLAUDE_MODEL` | `claude-sonnet-4-20250514` | Claude model for AI tasks |
| `MAX_RETRIES` | `3` | Max retries per company per stage |
| `LOG_FILE` | `output/pipeline.log` | Log file path |

## Resumability

The pipeline is fully resumable. Each company has a `stage` field in SQLite that tracks its progress. If the process is interrupted:

1. Already-processed companies are skipped on restart
2. Raw API/scraping responses are cached in the database — retries skip HTTP calls
3. SQLite WAL mode ensures no data corruption on kill

Just re-run `uv run python pipeline.py` to continue.

## Runtime Estimates

| Companies | Northdata (2.5s/req) | Fallback APIs | AI calls | Total |
|-----------|---------------------|---------------|----------|-------|
| ~700 | ~30 min | ~10 min | ~15 min | ~1 hour |
| ~3,000 | ~2 hours | ~30 min | ~45 min | ~3-4 hours |
