"""Stage 4b: Corporate structure traversal.

For companies with complex holding structures (GmbH & Co. KG, etc.),
follows Northdata links from corporate entity officers to find:
1. The actual Geschäftsführer / managing director (from the Komplementär-GmbH)
2. Financial data from related operative entities
3. A Claude-generated summary of the corporate structure
"""

from __future__ import annotations

import json
import logging

import config
import db
from clients import claude_ai
from clients.northdata_browser import NorthdataClient
from models import STAGE_PENDING_STRUCTURE, STAGE_PENDING_AI
from stages.s04_ceo_lookup import _extract_ceo_from_officers, _is_entity
from utils.logging_setup import ProgressTracker
from utils.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)

# Max depth of corporate tree traversal (entry → related entity → stop)
MAX_TRAVERSAL_DEPTH = 2


async def run() -> None:
    """Traverse corporate structures for companies pending structure analysis."""
    companies = db.get_pending(STAGE_PENDING_STRUCTURE, limit=10000)
    if not companies:
        logger.info("Stage 4b: No companies pending structure traversal")
        return

    logger.info("=" * 40)
    logger.info("Stage 4b: Corporate Structure Traversal (%d companies)", len(companies))
    logger.info("=" * 40)

    # Check if any companies have entity officers with Northdata URLs to follow
    needs_scraping = any(_get_entity_officers(c) for c in companies)

    tracker = ProgressTracker(len(companies), "structure")
    results = {"traversed": 0, "ceo_found": 0, "financials_found": 0, "skipped": 0, "error": 0}

    client = None
    rate_limiter = None
    if needs_scraping:
        client = NorthdataClient(
            email=config.NORTHDATA_EMAIL,
            password=config.NORTHDATA_PASSWORD,
            retry_attempts=config.NORTHDATA_RETRY_ATTEMPTS,
        )
        await client.start()
        rate_limiter = RateLimiter(config.NORTHDATA_DELAY_MIN, config.NORTHDATA_DELAY_MAX)

    try:
        for company in companies:
            try:
                entity_officers = _get_entity_officers(company)
                if not entity_officers:
                    # No corporate entities to traverse — pass through
                    company.stage = STAGE_PENDING_AI
                    db.update_company(company)
                    results["skipped"] += 1
                    tracker.tick(company.name_original, "no entities to traverse")
                    continue

                # Traverse corporate structure
                traversal_data = await _traverse_structure(
                    company, entity_officers, client, rate_limiter,
                )
                results["traversed"] += 1

                # Apply discovered CEO if we didn't have one (or had low confidence)
                if traversal_data.get("ceo") and (
                    not company.ceo_name or company.ceo_confidence in ("not found", "medium")
                ):
                    company.ceo_name = traversal_data["ceo"]["name"]
                    company.ceo_current_title = traversal_data["ceo"]["role"]
                    company.ceo_confidence = "high"
                    results["ceo_found"] += 1
                    logger.info(
                        "  Structure traversal found CEO for '%s': %s (%s)",
                        company.name_original, company.ceo_name, company.ceo_current_title,
                    )

                # Apply discovered financials if we were missing them
                if not company.revenue and traversal_data.get("financials"):
                    fin = traversal_data["financials"]
                    if fin.get("revenue"):
                        company.revenue = fin["revenue"]
                    if fin.get("employees_count"):
                        company.employees_count = fin["employees_count"]
                    if fin.get("total_assets"):
                        company.total_assets = fin["total_assets"]
                    results["financials_found"] += 1

                # Generate corporate structure summary via Claude
                try:
                    summary = await claude_ai.summarize_corporate_structure(
                        company_name=company.matched_name or company.name_original,
                        legal_form=company.legal_form,
                        country=company.country,
                        revenue=company.revenue,
                        employees=company.employees_count or company.employees_range,
                        ceo_name=company.ceo_name,
                        ceo_title=company.ceo_current_title,
                        related_entities=traversal_data.get("related_entities", []),
                    )
                    if summary:
                        company.corporate_structure_summary = summary
                except Exception as e:
                    logger.warning(
                        "Structure summary generation failed for '%s': %s",
                        company.name_original, e,
                    )

                company.stage = STAGE_PENDING_AI
                db.update_company(company)

                action_parts = []
                if traversal_data.get("ceo"):
                    action_parts.append(f"CEO: {traversal_data['ceo']['name']}")
                if company.corporate_structure_summary:
                    action_parts.append("summary")
                tracker.tick(
                    company.name_original,
                    ", ".join(action_parts) if action_parts else "traversed",
                )

            except Exception as e:
                logger.error("Structure traversal error for '%s': %s", company.name_original, e)
                db.mark_failed(company.id, str(e))
                results["error"] += 1

    finally:
        if client:
            await client.stop()

    tracker.summary(results)


def _get_entity_officers(company) -> list[dict]:
    """Extract officers that are corporate entities with Northdata URLs."""
    if not company.officers:
        return []

    try:
        officers = json.loads(company.officers)
    except (json.JSONDecodeError, TypeError):
        return []

    return [
        o for o in officers
        if _is_entity(o.get("name", "")) and o.get("northdata_url")
    ]


async def _traverse_structure(
    company,
    entity_officers: list[dict],
    client: NorthdataClient | None,
    rate_limiter: RateLimiter | None,
) -> dict:
    """Follow Northdata links for corporate entity officers.

    Returns dict with:
        - ceo: {name, role} or None
        - financials: {revenue, employees_count, ...} or None
        - related_entities: list of {name, role, officers, financials_summary}
    """
    result = {
        "ceo": None,
        "financials": None,
        "related_entities": [],
    }

    if not client:
        return result

    for entity in entity_officers:
        url = entity["northdata_url"]
        entity_name = entity.get("name", "?")
        entity_role = entity.get("role", "?")

        logger.info(
            "  Following %s link: %s → %s",
            entity_role, company.name_original, entity_name,
        )

        try:
            if rate_limiter:
                await rate_limiter.wait()

            scraped = await client.scrape_company_url(url)

            # Record for summary generation
            related = {
                "name": scraped.get("name") or entity_name,
                "role": entity_role,
                "legal_form": scraped.get("legal_form"),
                "status": scraped.get("status"),
            }

            # Extract CEO from the related entity's officers
            related_officers = scraped.get("officers", [])
            if related_officers:
                officers_json = json.dumps(related_officers, ensure_ascii=False)
                ceo = _extract_ceo_from_officers(officers_json)
                if ceo and not result["ceo"]:
                    result["ceo"] = ceo
                    related["ceo_found"] = ceo["name"]

                # Store officer names for summary
                person_officers = [
                    o.get("name") for o in related_officers
                    if not _is_entity(o.get("name", ""))
                ]
                if person_officers:
                    related["officers"] = person_officers

            # Check for financials
            if scraped.get("revenue") or scraped.get("employees_count"):
                fin = {}
                for key in ("revenue", "employees_count", "total_assets", "equity",
                            "employees_range", "revenue_range"):
                    if scraped.get(key):
                        fin[key] = scraped[key]
                if fin and not result["financials"]:
                    result["financials"] = fin
                    related["has_financials"] = True

            result["related_entities"].append(related)

            logger.info(
                "  Scraped %s: officers=%d, revenue=%s, employees=%s",
                entity_name,
                len(related_officers),
                scraped.get("revenue") or "?",
                scraped.get("employees_count") or "?",
            )

        except Exception as e:
            logger.warning(
                "  Failed to scrape entity '%s' for '%s': %s",
                entity_name, company.name_original, e,
            )
            result["related_entities"].append({
                "name": entity_name,
                "role": entity_role,
                "error": str(e),
            })

    return result
