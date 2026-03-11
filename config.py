"""Central configuration loaded from .env file."""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Project root
BASE_DIR = Path(__file__).parent

# API keys
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
BRAVE_API_KEY = os.environ.get("BRAVE_API_KEY", "")
PAPPERS_API_KEY = os.environ.get("PAPPERS_API_KEY", "")
OPENCORPORATES_API_KEY = os.environ.get("OPENCORPORATES_API_KEY", "")

# Rate limits
NORTHDATA_DELAY_SECONDS = float(os.environ.get("NORTHDATA_DELAY_SECONDS", "2.5"))

# Paths
INPUT_PDF = os.environ.get("INPUT_PDF", "input/companies.pdf")
OUTPUT_CSV = os.environ.get("OUTPUT_CSV", "output/companies_enriched.csv")
DB_PATH = os.environ.get("DB_PATH", "data/pipeline.db")
LOG_FILE = os.environ.get("LOG_FILE", "output/pipeline.log")

# PDF parsing
PDF_LAYOUT = os.environ.get("PDF_LAYOUT", "airbus_suppliers")

# Claude
CLAUDE_MODEL = os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-20250514")

# Pipeline
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
