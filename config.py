"""Central configuration loaded from .env file."""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Project root
BASE_DIR = Path(__file__).parent

# API keys
BRAVE_API_KEY = os.environ.get("BRAVE_API_KEY", "")
PAPPERS_API_KEY = os.environ.get("PAPPERS_API_KEY", "")
OPENCORPORATES_API_KEY = os.environ.get("OPENCORPORATES_API_KEY", "")

# Northdata
NORTHDATA_DELAY_MIN = float(os.environ.get("NORTHDATA_DELAY_MIN", "3.0"))
NORTHDATA_DELAY_MAX = float(os.environ.get("NORTHDATA_DELAY_MAX", "8.0"))
# Legacy fallback — used only if the caller still references NORTHDATA_DELAY_SECONDS
NORTHDATA_DELAY_SECONDS = float(os.environ.get("NORTHDATA_DELAY_SECONDS", "5.0"))
NORTHDATA_EMAIL = os.environ.get("NORTHDATA_EMAIL", "")
NORTHDATA_PASSWORD = os.environ.get("NORTHDATA_PASSWORD", "")
NORTHDATA_RETRY_ATTEMPTS = int(os.environ.get("NORTHDATA_RETRY_ATTEMPTS", "2"))

# Paths
INPUT_PDF = os.environ.get("INPUT_PDF", "input/companies.pdf")
OUTPUT_CSV = os.environ.get("OUTPUT_CSV", "output/companies_enriched.csv")
DB_PATH = os.environ.get("DB_PATH", "data/pipeline.db")
LOG_FILE = os.environ.get("LOG_FILE", "output/pipeline.log")

# PDF parsing
PDF_LAYOUT = os.environ.get("PDF_LAYOUT", "airbus_suppliers")

# Pipeline
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))


def derive_paths(input_path: str) -> tuple[str, str]:
    """Derive output CSV and DB paths from input filename.

    Example: "input/Boeing suppliers.xlsx" →
        ("output/Boeing suppliers_enriched.csv", "data/Boeing suppliers.db")
    """
    stem = Path(input_path).stem
    return f"output/{stem}_enriched.csv", f"data/{stem}.db"
