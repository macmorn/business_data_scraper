"""Anthropic Claude API wrapper for disambiguation and career summaries."""

from __future__ import annotations

import asyncio
import json
import logging

import anthropic

import config
from utils.retry import with_retry

logger = logging.getLogger(__name__)

# Semaphore to limit concurrent Claude API calls
_semaphore = asyncio.Semaphore(5)


def _get_client() -> anthropic.AsyncAnthropic:
    """Create an async Anthropic client."""
    if not config.ANTHROPIC_API_KEY:
        raise RuntimeError("ANTHROPIC_API_KEY not set")
    return anthropic.AsyncAnthropic(api_key=config.ANTHROPIC_API_KEY)


@with_retry(max_attempts=3, base_delay=2.0, exceptions=(anthropic.APIError,))
async def disambiguate(
    original_name: str,
    candidates: list[dict],
    context_hints: dict | None = None,
) -> dict:
    """Use Claude to pick the best match from multiple Northdata results.

    Returns dict with 'index' (0-based), 'confidence' (0-1), 'reasoning'.
    Returns index=-1 if no good match.
    """
    hints_text = ""
    if context_hints:
        hints_text = f"\nAdditional context: {json.dumps(context_hints)}"

    candidates_text = ""
    for i, c in enumerate(candidates):
        candidates_text += (
            f"\n{i}. {c.get('name', '?')} | {c.get('country', '?')} | "
            f"{c.get('legal_form', '?')} | {c.get('address', '?')} | "
            f"status: {c.get('status', '?')}"
        )

    prompt = f"""Given the company name "{original_name}", which of these search results is the best match?
Consider name similarity, country, active status, and address plausibility.
{hints_text}

Candidates:{candidates_text}

Respond as JSON only: {{"index": N, "confidence": 0.0-1.0, "reasoning": "brief explanation"}}
If none are a good match, use index: -1."""

    async with _semaphore:
        client = _get_client()
        response = await client.messages.create(
            model=config.CLAUDE_MODEL,
            max_tokens=200,
            messages=[{"role": "user", "content": prompt}],
        )

    text = response.content[0].text.strip()
    if "```" in text:
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        logger.warning("Failed to parse Claude disambiguation response: %s", text)
        return {"index": -1, "confidence": 0.0, "reasoning": "Failed to parse response"}


@with_retry(max_attempts=3, base_delay=2.0, exceptions=(anthropic.APIError,))
async def generate_career_summary(
    ceo_name: str,
    ceo_title: str,
    company_name: str,
    profile_data: dict | None = None,
) -> str:
    """Generate a 2-3 sentence career summary for a CEO."""
    profile_text = ""
    if profile_data:
        profile_text = f"\nAvailable profile data: {json.dumps(profile_data, ensure_ascii=False)}"

    prompt = f"""Write a 2-3 sentence professional career summary for {ceo_name}, currently {ceo_title} at {company_name}.
{profile_text}

Focus on: career trajectory, notable companies, domain expertise.
If limited information is available, write what you can based on the role and company.
Write in third person. Be concise and factual."""

    async with _semaphore:
        client = _get_client()
        response = await client.messages.create(
            model=config.CLAUDE_MODEL,
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}],
        )

    return response.content[0].text.strip()


@with_retry(max_attempts=3, base_delay=2.0, exceptions=(anthropic.APIError,))
async def extract_ceo_from_text(
    company_name: str,
    text: str,
) -> dict | None:
    """Use Claude to extract CEO/leader name from unstructured text.

    Returns dict with 'name' and 'title', or None.
    """
    prompt = f"""From the following text about {company_name}, extract the CEO, Geschäftsführer, Managing Director, or primary operational leader.

Text:
{text[:3000]}

Respond as JSON only: {{"name": "Full Name", "title": "Their Title"}}
If no leader can be identified, respond: {{"name": null, "title": null}}"""

    async with _semaphore:
        client = _get_client()
        response = await client.messages.create(
            model=config.CLAUDE_MODEL,
            max_tokens=100,
            messages=[{"role": "user", "content": prompt}],
        )

    result_text = response.content[0].text.strip()
    if "```" in result_text:
        result_text = result_text.split("```")[1]
        if result_text.startswith("json"):
            result_text = result_text[4:]
    try:
        result = json.loads(result_text)
        if result.get("name"):
            return result
    except json.JSONDecodeError:
        logger.warning("Failed to parse Claude CEO extraction response: %s", result_text)

    return None
