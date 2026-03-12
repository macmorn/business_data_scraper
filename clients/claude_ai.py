"""Claude Agent SDK wrapper for disambiguation and career summaries.

Uses the Agent SDK which runs through Claude Code (included with Max plan),
instead of the Anthropic API which bills separately per-token.
"""

from __future__ import annotations

import asyncio
import json
import logging

from claude_agent_sdk import (
    query,
    ClaudeAgentOptions,
    CLIConnectionError,
    ProcessError,
    ResultMessage,
)

from utils.retry import with_retry

logger = logging.getLogger(__name__)

# Semaphore to limit concurrent Claude calls
_semaphore = asyncio.Semaphore(5)


async def _ask_claude(
    prompt: str,
    system_prompt: str | None = None,
    output_format: dict | None = None,
) -> str:
    """Send a prompt to Claude via the Agent SDK and return the text result."""
    options = ClaudeAgentOptions(
        allowed_tools=[],
        max_turns=1,
    )
    if system_prompt:
        options.system_prompt = system_prompt
    if output_format:
        options.output_format = output_format

    async with _semaphore:
        async for message in query(prompt=prompt, options=options):
            if isinstance(message, ResultMessage):
                return message.result

    return ""


@with_retry(max_attempts=3, base_delay=2.0, exceptions=(CLIConnectionError, ProcessError))
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

If none are a good match, use index: -1."""

    result = await _ask_claude(
        prompt=prompt,
        output_format={
            "type": "json_schema",
            "schema": {
                "type": "object",
                "properties": {
                    "index": {"type": "integer"},
                    "confidence": {"type": "number"},
                    "reasoning": {"type": "string"},
                },
                "required": ["index", "confidence", "reasoning"],
                "additionalProperties": False,
            },
        },
    )

    try:
        return json.loads(result)
    except (json.JSONDecodeError, TypeError):
        logger.warning("Failed to parse Claude disambiguation response: %s", result)
        return {"index": -1, "confidence": 0.0, "reasoning": "Failed to parse response"}


@with_retry(max_attempts=3, base_delay=2.0, exceptions=(CLIConnectionError, ProcessError))
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

    return await _ask_claude(prompt=prompt)


@with_retry(max_attempts=3, base_delay=2.0, exceptions=(CLIConnectionError, ProcessError))
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

If no leader can be identified, use null values."""

    result = await _ask_claude(
        prompt=prompt,
        output_format={
            "type": "json_schema",
            "schema": {
                "type": "object",
                "properties": {
                    "name": {"type": ["string", "null"]},
                    "title": {"type": ["string", "null"]},
                },
                "required": ["name", "title"],
                "additionalProperties": False,
            },
        },
    )

    try:
        parsed = json.loads(result)
        if parsed.get("name"):
            return parsed
    except (json.JSONDecodeError, TypeError):
        logger.warning("Failed to parse Claude CEO extraction response: %s", result)

    return None
