"""Anthropic Claude API client for disambiguation and career summaries."""

from __future__ import annotations

import asyncio
import json
import logging

import anthropic

import config

logger = logging.getLogger(__name__)

# Semaphore to limit concurrent Claude API calls
_semaphore = asyncio.Semaphore(5)


def _get_client() -> anthropic.AsyncAnthropic:
    """Create an async Anthropic client."""
    if not config.ANTHROPIC_API_KEY:
        raise RuntimeError("ANTHROPIC_API_KEY not set")
    return anthropic.AsyncAnthropic(api_key=config.ANTHROPIC_API_KEY)


async def disambiguate_company(
    original_name: str,
    candidates: list[dict],
    context_hints: str | None = None,
) -> dict:
    """
    Use Claude to pick the best matching company from multiple candidates.

    Returns dict with:
        - "index": int (0-based index of best match, or -1 if unresolvable)
        - "confidence": float (0.0-1.0)
        - "reasoning": str
    """
    prompt = f"""You are helping match a company name from a PDF to database search results.

Original company name from PDF: "{original_name}"
{f'Additional context: {context_hints}' if context_hints else ''}

Here are the search result candidates:
{json.dumps(candidates, indent=2, ensure_ascii=False)}

Analyze each candidate and determine which one best matches the original company name.
Consider: name similarity, country, legal form, active status, and any context hints.

Respond with ONLY valid JSON (no markdown, no code fences):
{{"index": <0-based index of best match or -1 if none match well>, "confidence": <0.0 to 1.0>, "reasoning": "<brief explanation>"}}"""

    async with _semaphore:
        client = _get_client()
        try:
            response = await client.messages.create(
                model=config.CLAUDE_MODEL,
                max_tokens=300,
                messages=[{"role": "user", "content": prompt}],
            )

            text = response.content[0].text.strip()
            # Try to parse JSON response
            result = json.loads(text)
            return {
                "index": int(result.get("index", -1)),
                "confidence": float(result.get("confidence", 0.0)),
                "reasoning": result.get("reasoning", ""),
            }
        except json.JSONDecodeError:
            logger.warning("Claude returned non-JSON for disambiguation: %s", text[:200])
            return {"index": -1, "confidence": 0.0, "reasoning": "Failed to parse AI response"}
        except Exception as e:
            logger.error("Claude API error during disambiguation: %s", e)
            return {"index": -1, "confidence": 0.0, "reasoning": str(e)}


async def generate_career_summary(
    ceo_name: str,
    company_name: str,
    title: str | None = None,
    additional_info: str | None = None,
) -> str | None:
    """
    Generate a 2-3 sentence career summary for a CEO.

    Returns summary string or None on failure.
    """
    info_parts = []
    if title:
        info_parts.append(f"Current title: {title}")
    if additional_info:
        info_parts.append(f"Additional info: {additional_info}")

    info_str = "\n".join(info_parts) if info_parts else "No additional information available."

    prompt = f"""Write a concise 2-3 sentence professional career summary for {ceo_name}, who is associated with {company_name}.

{info_str}

Write the summary in third person. Focus on their role and the company. If you don't have enough information for a detailed summary, write a brief factual statement about their current role.

Respond with ONLY the summary text, no preamble or explanation."""

    async with _semaphore:
        client = _get_client()
        try:
            response = await client.messages.create(
                model=config.CLAUDE_MODEL,
                max_tokens=200,
                messages=[{"role": "user", "content": prompt}],
            )

            summary = response.content[0].text.strip()
            # Basic sanity check
            if len(summary) < 20 or len(summary) > 500:
                logger.warning("Career summary unusual length (%d chars) for %s", len(summary), ceo_name)
            return summary
        except Exception as e:
            logger.error("Claude API error during career summary for %s: %s", ceo_name, e)
            return None


async def extract_ceo_from_text(
    company_name: str,
    page_text: str,
) -> dict | None:
    """
    Use Claude to extract CEO/leader info from website text.

    Returns dict with: name, title, or None if not found.
    """
    # Truncate text to avoid excessive token usage
    truncated = page_text[:3000]

    prompt = f"""From the following website text for the company "{company_name}", identify the CEO, Managing Director, Geschäftsführer, or primary operational leader.

Website text:
---
{truncated}
---

If you can identify the CEO/leader, respond with ONLY valid JSON:
{{"name": "Full Name", "title": "Their Title"}}

If you cannot identify the leader with reasonable confidence, respond with:
{{"name": null, "title": null}}"""

    async with _semaphore:
        client = _get_client()
        try:
            response = await client.messages.create(
                model=config.CLAUDE_MODEL,
                max_tokens=100,
                messages=[{"role": "user", "content": prompt}],
            )

            text = response.content[0].text.strip()
            result = json.loads(text)
            if result.get("name"):
                return {"name": result["name"], "title": result.get("title")}
            return None
        except Exception as e:
            logger.error("Claude API error during CEO extraction for %s: %s", company_name, e)
            return None
