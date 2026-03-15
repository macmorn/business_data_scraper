"""Claude Agent SDK wrapper for disambiguation and career summaries.

Uses the Agent SDK which runs through Claude Code (included with Max plan),
instead of the Anthropic API which bills separately per-token.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re

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
    use_web: bool = False,
) -> str:
    """Send a prompt to Claude via the Agent SDK and return the text result.

    Args:
        use_web: If True, enables WebSearch and WebFetch tools with up to 3
                 turns so Claude can research online before responding.
    """
    options = ClaudeAgentOptions(
        allowed_tools=["WebSearch", "WebFetch"] if use_web else [],
        max_turns=6 if use_web else 1,
    )
    if system_prompt:
        options.system_prompt = system_prompt
    if output_format:
        options.output_format = output_format

    async with _semaphore:
        result = ""
        async for message in query(prompt=prompt, options=options):
            if isinstance(message, ResultMessage):
                result = message.result
        return result


@with_retry(
    max_attempts=2, base_delay=2.0, exceptions=(CLIConnectionError, ProcessError)
)
async def resolve_company_name(
    company_name: str,
    country_hint: str | None = None,
) -> dict:
    """Use Claude with web search to find the correct company name and northdata URL.

    Useful when the company name in our input doesn't match northdata's listing
    (e.g. word order, abbreviations, legal form differences).

    Returns dict with 'resolved_name' (str|None), 'northdata_url' (str|None), 'reasoning' (str).
    """
    country_text = f" The company is likely in {country_hint}." if country_hint else ""

    prompt = f"""I need to find the company "{company_name}" on northdata.com.{country_text}

Search for this company on northdata.com. The company name in our records may have different word order,
abbreviations, or legal form than the official name on northdata.

Steps:
1. Search for "{company_name} northdata" on a search engine linke google to find the northdata.com page
2. If not found directly, try variations of the name (reorder words, expand abbreviations)
3. Return the official company name as shown on northdata and the full northdata.com URL

If you absolutely cannot find this company on northdata, set both to null.

You MUST respond with ONLY a JSON object in this exact format (no markdown, no explanation):
{{"resolved_name": "Official Company Name" or null, "northdata_url": "https://www.northdata.com/..." or null, "reasoning": "brief explanation"}}"""

    result = await _ask_claude(
        prompt=prompt,
        use_web=True,
    )

    try:
        return json.loads(result)
    except (json.JSONDecodeError, TypeError):
        logger.warning("Failed to parse Claude name resolution response: %s", result)
        return {
            "resolved_name": None,
            "northdata_url": None,
            "reasoning": "Failed to parse response",
        }


@with_retry(
    max_attempts=3, base_delay=2.0, exceptions=(CLIConnectionError, ProcessError)
)
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
        details = c.get("details", "")
        details_str = f" ({details})" if details else ""
        candidates_text += f"\n{i}. {c.get('name', '?')}{details_str}"

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


@with_retry(
    max_attempts=3, base_delay=2.0, exceptions=(CLIConnectionError, ProcessError)
)
async def generate_career_summary(
    ceo_name: str,
    ceo_title: str,
    company_name: str,
    profile_data: dict | None = None,
) -> str:
    """Generate a 2-3 sentence career summary for a CEO."""
    profile_text = ""
    if profile_data:
        profile_text = (
            f"\nAvailable profile data: {json.dumps(profile_data, ensure_ascii=False)}"
        )

    prompt = f"""Write a 2-3 sentence professional career summary for {ceo_name}, currently {ceo_title} at {company_name}.
{profile_text}

Focus on: career trajectory, notable companies, domain expertise.
If limited information is available, write what you can based on the role and company.
Write in third person. Be concise and factual."""

    return await _ask_claude(prompt=prompt)


def _extract_linkedin_url(text: str) -> str | None:
    """Extract a LinkedIn profile URL from text."""
    match = re.search(r'https?://(?:[a-z]{2,3}\.)?linkedin\.com/in/[^\s\)"\]\',]+', text)
    return match.group(0).rstrip(".,;:") if match else None


def _strip_markdown(text: str) -> str:
    """Strip markdown formatting from text for use as plain summary."""
    # Remove **bold** markers and their labels
    text = re.sub(r"\*\*[^*]+\*\*:?\s*", "", text)
    # Convert [text](url) links to just text
    text = re.sub(r"\[([^\]]+)\]\([^\)]+\)", r"\1", text)
    # Remove bullet markers
    text = re.sub(r"^[\s]*[-*]\s+", "", text, flags=re.M)
    # Collapse whitespace
    text = re.sub(r"\n{2,}", " ", text).strip()
    return text


def _try_parse_json(text: str) -> dict | None:
    """Try to parse JSON from text, including extracting JSON from markdown code blocks."""
    if not text:
        return None
    # Try direct parse
    try:
        return json.loads(text)
    except (json.JSONDecodeError, TypeError):
        pass
    # Try extracting from markdown code block
    match = re.search(r"```(?:json)?\s*\n?(.*?)\n?```", text, re.S)
    if match:
        try:
            return json.loads(match.group(1))
        except (json.JSONDecodeError, TypeError):
            pass
    # Try finding a JSON object in the text
    match = re.search(r"\{[^{}]*\}", text, re.S)
    if match:
        try:
            return json.loads(match.group(0))
        except (json.JSONDecodeError, TypeError):
            pass
    return None


@with_retry(
    max_attempts=3, base_delay=2.0, exceptions=(CLIConnectionError, ProcessError)
)
async def research_ceo(
    ceo_name: str,
    ceo_title: str,
    company_name: str,
) -> dict:
    """Research a CEO online — find LinkedIn URL and generate career summary.

    Uses web search to find real career information and LinkedIn profile.
    Returns dict with 'linkedin_url' (str|None) and 'career_summary' (str).
    """
    prompt = f"""Research {ceo_name}, currently {ceo_title} at {company_name}.

1. Search for their LinkedIn profile URL (search: {ceo_name} {company_name} LinkedIn)
2. Write a 2-3 sentence professional career summary based on what you find

Focus on: career trajectory, domain expertise, notable companies.
Write in third person. Be concise and factual.
If you cannot find a LinkedIn profile, set linkedin_url to null.

You MUST respond with ONLY a JSON object in this exact format (no markdown, no explanation):
{{"linkedin_url": "https://linkedin.com/in/..." or null, "career_summary": "2-3 sentence summary"}}"""

    result = await _ask_claude(
        prompt=prompt,
        use_web=True,
    )

    # Try JSON first
    parsed = _try_parse_json(result)
    if parsed and ("linkedin_url" in parsed or "career_summary" in parsed):
        return {
            "linkedin_url": parsed.get("linkedin_url"),
            "career_summary": parsed.get("career_summary", ""),
        }

    # Fallback: extract from text response
    if result:
        linkedin_url = _extract_linkedin_url(result)
        career = _strip_markdown(result)
        # Remove the LinkedIn URL line from the summary
        career = re.sub(r"https?://\S+", "", career).strip()
        if len(career) > 500:
            career = career[:497] + "..."
        return {"linkedin_url": linkedin_url, "career_summary": career}

    return {"linkedin_url": None, "career_summary": ""}


@with_retry(
    max_attempts=3, base_delay=2.0, exceptions=(CLIConnectionError, ProcessError)
)
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


@with_retry(
    max_attempts=2, base_delay=2.0, exceptions=(CLIConnectionError, ProcessError)
)
async def discover_ceo(
    company_name: str,
    country: str | None = None,
    legal_form: str | None = None,
) -> dict | None:
    """Use Claude + web search to discover the CEO/managing director of a company.

    Used when Stage 4 could not identify a CEO from registry data.
    """
    context = ""
    if country:
        context += f" The company is based in {country}."
    if legal_form and "co. kg" in (legal_form or "").lower():
        context += (
            " This is a GmbH & Co. KG structure. The actual managing director"
            " (Geschäftsführer) is typically found in the Komplementär-GmbH"
            " (the general partner company). Look for the Geschäftsführer of"
            " the Verwaltungsgesellschaft or Komplementär."
        )

    prompt = f"""Find the current CEO, Geschäftsführer, or managing director of "{company_name}".{context}

Search for:
1. "{company_name}" CEO OR Geschäftsführer OR managing director
2. If it's a GmbH & Co. KG, search for the Komplementär/Verwaltungsgesellschaft's Geschäftsführer
3. Check the company website's imprint (Impressum) or leadership page
4. Also search for their LinkedIn profile

You MUST respond with ONLY a JSON object in this exact format (no markdown, no explanation):
{{"name": "Person Name" or null, "title": "Their Title" or null, "career_summary": "1-2 sentences" or null, "linkedin_url": "https://..." or null}}"""

    result = await _ask_claude(
        prompt=prompt,
        use_web=True,
    )

    # Try JSON first
    parsed = _try_parse_json(result)
    if parsed and parsed.get("name"):
        return parsed

    # Fallback: extract what we can from text
    if result and len(result) > 10:
        linkedin_url = _extract_linkedin_url(result)
        career = _strip_markdown(result)
        if len(career) > 500:
            career = career[:497] + "..."
        # Can't reliably extract name from unstructured text — return None
        logger.warning("CEO discovery returned text, not JSON: %s", result[:200])

    return None


@with_retry(
    max_attempts=2, base_delay=2.0, exceptions=(CLIConnectionError, ProcessError)
)
async def enrich_missing_financials(
    company_name: str,
    country: str | None = None,
    existing_data: dict | None = None,
) -> dict:
    """Use Claude + web search to find missing financial metrics.

    Used when Northdata didn't have financial data (common for non-German companies).
    """
    known = ""
    if existing_data:
        known_items = [f"- {k}: {v}" for k, v in existing_data.items() if v]
        if known_items:
            known = "\nAlready known:\n" + "\n".join(known_items)

    country_text = f" based in {country}" if country else ""

    prompt = f"""Find key financial and operational metrics for "{company_name}"{country_text}.
{known}

Search for:
1. Employee count (approximate or exact)
2. Revenue / turnover (most recent available)
3. Any other financial data available publicly

Look at: company website, annual reports, business registries, press releases, industry databases.
Only include data you can actually verify from search results. Use null for anything you cannot find.

You MUST respond with ONLY a JSON object in this exact format (no markdown, no explanation):
{{"employees_count": "number or range" or null, "revenue": "amount with currency" or null, "total_assets": "amount" or null, "source_notes": "where you found this"}}"""

    result = await _ask_claude(
        prompt=prompt,
        use_web=True,
    )

    parsed = _try_parse_json(result)
    if parsed:
        return parsed

    # Fallback: try to extract numbers from text
    if result:
        data = {}
        emp_match = re.search(r"(\d[\d,.']+)\s*(?:employees|Mitarbeiter|staff|people)", result, re.I)
        if emp_match:
            data["employees_count"] = emp_match.group(1).replace("'", ",")
        rev_match = re.search(r"(?:revenue|turnover|Umsatz)[:\s]*([€$£]\s*[\d.,]+\s*[BMKbmk]?(?:illion)?)", result, re.I)
        if rev_match:
            data["revenue"] = rev_match.group(1).strip()
        if data:
            logger.info("Extracted financials from text: %s", data)
            return data
        logger.warning("Could not extract financials from response: %s", result[:200])

    return {}


@with_retry(
    max_attempts=2, base_delay=2.0, exceptions=(CLIConnectionError, ProcessError)
)
async def estimate_employee_count(
    company_name: str,
    country: str | None = None,
    revenue: str | None = None,
) -> str | None:
    """Use Claude + web search to estimate a company's employee count.

    Specifically targeted at finding headcount when Northdata lacks it.
    Returns employee count as string, or None.
    """
    context = ""
    if country:
        context += f" The company is based in {country}."
    if revenue:
        context += f" Their revenue is approximately {revenue}."

    prompt = f"""Find the approximate number of employees at "{company_name}".{context}

Search specifically for:
1. "{company_name}" employees OR Mitarbeiter OR headcount
2. The company's LinkedIn page (shows employee count)
3. Company website "About us" / "Über uns" page
4. Business registry filings or annual reports
5. Press releases mentioning staff numbers

If you find a range (e.g. "200-500"), use the midpoint. If you find "approximately 300", use "~300".

You MUST respond with ONLY a JSON object (no markdown, no explanation):
{{"employees_count": "number" or null, "source": "where you found this"}}"""

    result = await _ask_claude(
        prompt=prompt,
        use_web=True,
    )

    parsed = _try_parse_json(result)
    if parsed and parsed.get("employees_count"):
        return str(parsed["employees_count"])

    # Fallback: extract number from text
    if result:
        match = re.search(r"(\d[\d,.']+)\s*(?:employees|Mitarbeiter|staff|people)", result, re.I)
        if match:
            return match.group(1).replace("'", ",")

    return None


@with_retry(
    max_attempts=2, base_delay=2.0, exceptions=(CLIConnectionError, ProcessError)
)
async def summarize_corporate_structure(
    company_name: str,
    legal_form: str | None = None,
    country: str | None = None,
    revenue: str | None = None,
    employees: str | None = None,
    ceo_name: str | None = None,
    ceo_title: str | None = None,
    related_entities: list[dict] | None = None,
) -> str | None:
    """Generate a 2-4 sentence narrative of the corporate structure.

    Synthesizes data we already have (no web search needed).
    """
    facts = [f"Company: {company_name}"]
    if legal_form:
        facts.append(f"Legal form: {legal_form}")
    if country:
        facts.append(f"Country: {country}")
    if revenue:
        facts.append(f"Revenue: {revenue}")
    if employees:
        facts.append(f"Employees: {employees}")
    if ceo_name:
        facts.append(f"CEO/Managing Director: {ceo_name} ({ceo_title or 'unknown title'})")

    if related_entities:
        for entity in related_entities:
            parts = [f"Related entity: {entity.get('name', '?')} (role: {entity.get('role', '?')})"]
            if entity.get("legal_form"):
                parts.append(f"legal form: {entity['legal_form']}")
            if entity.get("ceo_found"):
                parts.append(f"Geschäftsführer: {entity['ceo_found']}")
            if entity.get("officers"):
                parts.append(f"officers: {', '.join(entity['officers'][:5])}")
            if entity.get("has_financials"):
                parts.append("has financial data")
            facts.append(", ".join(parts))

    prompt = f"""Based on the following data about a company, write a 2-4 sentence summary
describing the business scale, corporate structure, and who is in charge.
Write in a professional, concise style suitable for a business research report.

Data:
{chr(10).join(facts)}

Focus on:
- Business scale (revenue, employees)
- Corporate structure (e.g. GmbH & Co. KG with Verwaltungsgesellschaft as Komplementär)
- Who is the operational leader and their role"""

    result = await _ask_claude(prompt=prompt)
    return result.strip() if result and result.strip() else None
