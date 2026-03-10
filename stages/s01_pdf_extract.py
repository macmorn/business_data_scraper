"""Stage 1: Extract company names from PDF, deduplicate, and load into queue."""

from __future__ import annotations

import logging
import re
from pathlib import Path

import pdfplumber

import config
import db
from models import STAGE_PENDING_NORTHDATA

logger = logging.getLogger(__name__)

# Legal form suffixes used to identify company names in text
LEGAL_FORMS = [
    # German
    "GmbH", "GmbH & Co. KG", "GmbH & Co. KGaA", "AG", "KG", "OHG", "e.K.", "UG",
    # Austrian
    "GesmbH",
    # Swiss
    "SA", "Sàrl",
    # French
    "SAS", "SARL", "S.A.", "S.A.S.", "S.A.R.L.", "SCA", "SNC",
    # Dutch / Belgian
    "B.V.", "BV", "N.V.", "NV", "VOF", "BVBA", "SPRL",
    # Italian
    "S.p.A.", "S.r.l.", "SPA", "SRL",
    # Spanish
    "S.L.", "S.A.",
    # General European
    "Ltd", "Ltd.", "Limited", "PLC", "SE", "SCE",
]

# Build regex pattern to match company names ending with legal forms
_legal_escaped = [re.escape(lf) for lf in sorted(LEGAL_FORMS, key=len, reverse=True)]
_legal_pattern = "|".join(_legal_escaped)
# Match: word characters/spaces/hyphens followed by a legal form
COMPANY_RE = re.compile(
    rf"([\w\s\-&.,/'+()]+?\s*(?:{_legal_pattern}))\b",
    re.IGNORECASE | re.UNICODE,
)


def extract_names_from_pdf(pdf_path: str) -> list[str]:
    """Extract company names from a PDF file."""
    path = Path(pdf_path)
    if not path.exists():
        raise FileNotFoundError(f"PDF not found: {pdf_path}")

    names = []
    with pdfplumber.open(str(path)) as pdf:
        logger.info("Processing PDF: %s (%d pages)", path.name, len(pdf.pages))
        for i, page in enumerate(pdf.pages):
            text = page.extract_text()
            if not text:
                continue
            # Try regex extraction first
            matches = COMPANY_RE.findall(text)
            for match in matches:
                name = _clean_name(match)
                if name and len(name) >= 3:
                    names.append(name)

            # Also try line-by-line extraction for simple lists
            for line in text.split("\n"):
                line = line.strip()
                if _looks_like_company(line):
                    name = _clean_name(line)
                    if name and len(name) >= 3:
                        names.append(name)

    logger.info("Extracted %d raw company names from PDF", len(names))
    return names


def _clean_name(name: str) -> str:
    """Clean up an extracted company name."""
    name = name.strip()
    # Remove leading numbers/bullets
    name = re.sub(r"^[\d\.\)\-\*•]+\s*", "", name)
    # Remove excessive whitespace
    name = re.sub(r"\s+", " ", name)
    return name.strip()


def _looks_like_company(line: str) -> bool:
    """Heuristic: does this line look like a company name?"""
    if not line or len(line) < 5 or len(line) > 200:
        return False
    # Check if line contains any legal form suffix
    line_upper = line.upper()
    for lf in LEGAL_FORMS:
        if lf.upper() in line_upper:
            return True
    return False


def deduplicate(names: list[str]) -> list[str]:
    """Deduplicate company names using normalized comparison."""
    seen = {}
    unique = []
    for name in names:
        key = _normalize_for_dedup(name)
        if key not in seen:
            seen[key] = name
            unique.append(name)
    return unique


def _normalize_for_dedup(name: str) -> str:
    """Normalize a name for deduplication comparison."""
    n = name.lower().strip()
    # Remove punctuation for comparison
    n = re.sub(r"[.,\-/'+()]", " ", n)
    n = re.sub(r"\s+", " ", n)
    return n.strip()


def run() -> None:
    """Execute PDF extraction stage."""
    logger.info("=" * 40)
    logger.info("Stage 1: PDF Extraction")
    logger.info("=" * 40)

    pdf_path = config.INPUT_PDF
    names = extract_names_from_pdf(pdf_path)
    unique_names = deduplicate(names)
    logger.info("After deduplication: %d unique companies", len(unique_names))

    count = db.load_companies(unique_names)
    logger.info("Loaded %d new companies into queue", count)

    # Advance all 'new' companies to pending_northdata
    with db._get_conn() as conn:
        conn.execute(
            "UPDATE companies SET stage = ? WHERE stage = 'new'",
            (STAGE_PENDING_NORTHDATA,),
        )
    logger.info("Stage 1 complete")
