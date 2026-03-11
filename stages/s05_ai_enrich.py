"""Stage 5: AI enrichment - disambiguation + career summaries.

Uses Claude API for:
1. Resolving multi-match Northdata results (companies with stage=pending_ai
   that have northdata_raw with multiple candidates)
2. Generating CEO career summaries for companies with CEO data
"""

from __future__ import annotations

import json
import logging

import db
from clients import claude_ai
from clients.northdata_browser import NorthdataClient
from models import STAGE_PENDING_AI, STAGE_PENDING_NORMALIZE, STAGE_PENDING_CEO
from utils.logging_setup import ProgressTracker

logger = logging.getLogger(__name__)


async def run() -> None:
    """Process all companies pending AI enrichment."""
    companies = db.get_pending(STAGE_PENDING_AI, limit=10000)
    if not companies:
        logger.info("Stage 5: No companies pending AI enrichment")
        return

    logger.info("=" * 40)
    logger.info("Stage 5: AI Enrichment (%d companies)", len(companies))
    logger.info("=" * 40)

    tracker = ProgressTracker(len(companies), "ai_enrich")
    results = {"disambiguated": 0, "summary_generated": 0, "skipped": 0, "error": 0}

    for company in companies:
        try:
            # Task 1: Disambiguation (if northdata returned multiple matches)
            if company.northdata_raw:
                raw = json.loads(company.northdata_raw)
                if raw.get("status") == "multiple" and raw.get("matches"):
                    await _disambiguate_company(company, raw["matches"])
                    results["disambiguated"] += 1

            # Task 2: Career summary (if we have a CEO name)
            if company.ceo_name and company.ceo_current_title:
                try:
                    summary = await claude_ai.generate_career_summary(
                        ceo_name=company.ceo_name,
                        ceo_title=company.ceo_current_title,
                        company_name=company.matched_name or company.name_original,
                    )
                    company.ceo_career_summary = summary
                    results["summary_generated"] += 1
                except Exception as e:
                    logger.warning("Career summary failed for %s: %s", company.ceo_name, e)

            company.stage = STAGE_PENDING_NORMALIZE
            db.update_company(company)

            action = []
            if company.ceo_career_summary:
                action.append("summary")
            tracker.tick(company.name_original, ", ".join(action) if action else "passed through")

        except Exception as e:
            logger.error("AI enrichment error for '%s': %s", company.name_original, e)
            db.mark_failed(company.id, str(e))
            results["error"] += 1

    tracker.summary(results)


async def _disambiguate_company(company, matches: list[dict]) -> None:
    """Use Claude to pick the best match from multiple candidates."""
    context_hints = {}
    if company.country:
        context_hints["country_hint"] = company.country
    if company.address:
        context_hints["address_hint"] = company.address

    result = await claude_ai.disambiguate(
        original_name=company.name_original,
        candidates=matches,
        context_hints=context_hints if context_hints else None,
    )

    idx = result.get("index", -1)
    confidence = result.get("confidence", 0.0)

    if idx >= 0 and idx < len(matches):
        chosen = matches[idx]
        company.matched_name = chosen.get("name")
        company.confidence_score = confidence
        if confidence < 0.7:
            company.needs_review_flag = True
        logger.info(
            "Disambiguated '%s' -> '%s' (confidence=%.2f)",
            company.name_original, company.matched_name, confidence,
        )
    else:
        company.needs_review_flag = True
        company.confidence_score = 0.0
        logger.info("Could not disambiguate '%s' - flagged for review", company.name_original)
