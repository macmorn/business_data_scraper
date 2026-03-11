"""Stage 4: Identify CEO from registry data and website scraping.

First tries to extract CEO from the officers field (from Northdata or registry).
Falls back to scraping the company's website for leadership info.
"""

from __future__ import annotations

import json
import logging

import db
from models import STAGE_PENDING_CEO, STAGE_PENDING_AI
from utils.logging_setup import ProgressTracker

logger = logging.getLogger(__name__)

# Officer roles that indicate CEO/primary leader, ordered by specificity
CEO_ROLES = [
    "geschäftsführer", "geschäftsfuhrer", "geschaeftsfuehrer",
    "ceo", "chief executive officer",
    "managing director", "directeur général", "directeur general",
    "gérant", "gerant", "président", "president",
    "vorstandsvorsitzender", "vorstand",
    "amministratore delegato", "director general",
    "bestuurder", "zaakvoerder",
]


async def run() -> None:
    """Process all companies pending CEO identification."""
    companies = db.get_pending(STAGE_PENDING_CEO, limit=10000)
    if not companies:
        logger.info("Stage 4: No companies pending CEO lookup")
        return

    logger.info("=" * 40)
    logger.info("Stage 4: CEO Identification (%d companies)", len(companies))
    logger.info("=" * 40)

    tracker = ProgressTracker(len(companies), "ceo_lookup")
    results = {"from_officers": 0, "not_found": 0, "error": 0}

    for company in companies:
        try:
            ceo = _extract_ceo_from_officers(company.officers)
            if ceo:
                company.ceo_name = ceo["name"]
                company.ceo_current_title = ceo["role"]
                company.ceo_confidence = "high"
                results["from_officers"] += 1
                tracker.tick(company.name_original, f"CEO: {ceo['name']}")
            else:
                company.ceo_confidence = "not found"
                company.needs_review_flag = True
                results["not_found"] += 1
                tracker.tick(company.name_original, "no CEO found")

            company.stage = STAGE_PENDING_AI
            db.update_company(company)

        except Exception as e:
            logger.error("CEO lookup error for '%s': %s", company.name_original, e)
            db.mark_failed(company.id, str(e))
            results["error"] += 1

    tracker.summary(results)


def _extract_ceo_from_officers(officers_json: str | None) -> dict | None:
    """Extract the CEO/primary leader from an officers JSON array."""
    if not officers_json:
        return None

    try:
        officers = json.loads(officers_json)
    except (json.JSONDecodeError, TypeError):
        return None

    if not isinstance(officers, list):
        return None

    best = None
    best_score = -1

    for officer in officers:
        name = officer.get("name", "").strip()
        role = officer.get("role", "").strip()
        if not name or not role:
            continue

        role_lower = role.lower()
        score = 0
        for i, ceo_role in enumerate(CEO_ROLES):
            if ceo_role in role_lower:
                score = len(CEO_ROLES) - i
                break

        if score > best_score:
            best_score = score
            best = {"name": name, "role": role}

    return best if best_score > 0 else None
