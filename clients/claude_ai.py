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
    ClaudeSDKClient,
    ClaudeAgentOptions,
    AssistantMessage,
    CLIConnectionError,
    ProcessError,
    ResultMessage,
)

from utils.retry import with_retry

logger = logging.getLogger(__name__)


class ClaudeTimeoutError(Exception):
    """Raised when a Claude Agent SDK call exceeds its timeout."""

    pass


class ClaudeUsageLimitError(Exception):
    """Raised when a Claude Agent SDK call hits a usage / rate / billing limit.

    Carries the SDK error subtype so the stage can record exactly why and leave
    the company in a re-runnable state. Deliberately NOT retried — retrying a
    usage limit immediately is pointless.
    """

    def __init__(self, subtype: str):
        self.subtype = subtype
        super().__init__(f"Claude usage limit reached (subtype={subtype})")


# Semaphore to limit concurrent Claude calls
_semaphore = asyncio.Semaphore(5)

# Model + reasoning effort applied to every Claude call (all helpers funnel
# through _ask_claude). Set explicitly to Sonnet / medium effort.
_MODEL = "sonnet"
_EFFORT = "medium"

# Timeouts for Claude calls (seconds)
_TIMEOUT_WEB = 200  # web-search enabled — network round-trips can be slow
_TIMEOUT_PLAIN = 60  # text generation only

# Substrings that mark a usage/rate/billing limit in SDK error text.
_LIMIT_MARKERS = ("usage limit", "rate limit", "rate_limit", "billing")
_CLIENT_DISCONNECT_TIMEOUT = 10


def _looks_like_limit(text: str | None) -> bool:
    """True if the SDK error text looks like a usage/rate/billing limit."""
    if not text:
        return False
    low = text.lower()
    return any(marker in low for marker in _LIMIT_MARKERS)


async def _disconnect_claude_client(
    client: ClaudeSDKClient,
    *,
    reason: str,
    propagate_cancel: bool = True,
) -> None:
    """Disconnect a Claude SDK client without creating a separate asyncio task.

    The SDK's anyio cancel scopes are task-affine. Cleanup must stay in the task
    that connected and consumed the client, otherwise the SDK can raise:
    "Attempted to exit cancel scope in a different task than it was entered in".
    `asyncio.timeout()` enforces a deadline while staying in the current task.
    """
    try:
        async with asyncio.timeout(_CLIENT_DISCONNECT_TIMEOUT):
            await client.disconnect()
    except asyncio.TimeoutError:
        logger.warning(
            "Claude client disconnect timed out after %ds during %s",
            _CLIENT_DISCONNECT_TIMEOUT,
            reason,
        )
    except asyncio.CancelledError:
        logger.warning("Claude client disconnect was cancelled during %s", reason)
        if propagate_cancel:
            raise
        task = asyncio.current_task()
        if task and task.cancelling():
            task.uncancel()
    except Exception as e:
        logger.warning("Claude client disconnect failed during %s: %r", reason, e)


async def _ask_claude(
    prompt: str,
    system_prompt: str | None = None,
    output_format: dict | None = None,
    use_web: bool = False,
) -> str:
    """Send a prompt to Claude via the Agent SDK and return the text result.

    Args:
        use_web: If True, enables WebSearch and WebFetch tools with up to 6
                 turns so Claude can research online before responding.
    """
    # Web calls need a generous turn budget: at high effort the model emits a
    # ThinkingBlock before each tool call, so a few web searches burn ~2 turns
    # each. Measured ~13 assistant turns for a focused single-topic search; 6
    # (and even 12) could run out before the final answer -> empty result. 18
    # leaves comfortable headroom and still finishes well under the timeout.
    options = ClaudeAgentOptions(
        allowed_tools=["WebSearch", "WebFetch"] if use_web else [],
        max_turns=18 if use_web else 1,
        model=_MODEL,
        effort=_EFFORT,
    )
    if system_prompt:
        options.system_prompt = system_prompt
    if output_format:
        options.output_format = output_format

    timeout = _TIMEOUT_WEB if use_web else _TIMEOUT_PLAIN

    async with _semaphore:
        result = ""
        client = ClaudeSDKClient(options=options)
        client_disconnected = False

        async def _disconnect(reason: str, *, propagate_cancel: bool = True) -> None:
            nonlocal client_disconnected
            if client_disconnected:
                return
            await _disconnect_claude_client(
                client,
                reason=reason,
                propagate_cancel=propagate_cancel,
            )
            client_disconnected = True

        async def _collect() -> None:
            nonlocal result
            model_logged = False
            await client.connect()
            await client.query(prompt)
            async for message in client.receive_response():
                # Record the model the SDK actually served (once per call), so
                # pipeline.log shows which model handled each request rather than
                # only the requested alias (_MODEL).
                if isinstance(message, AssistantMessage):
                    if not model_logged and message.model:
                        logger.info(
                            "Claude call served by model=%s (requested=%s, effort=%s, use_web=%s)",
                            message.model,
                            _MODEL,
                            _EFFORT,
                            use_web,
                        )
                        model_logged = True
                # Primary signal: the assistant-message error enum from the SDK.
                if isinstance(message, AssistantMessage) and message.error in (
                    "rate_limit",
                    "billing_error",
                ):
                    raise ClaudeUsageLimitError(message.error)
                if isinstance(message, ResultMessage):
                    if message.is_error and (
                        _looks_like_limit(message.subtype)
                        or _looks_like_limit(message.result)
                    ):
                        subtype = (
                            message.subtype or message.stop_reason or "result_error"
                        )
                        raise ClaudeUsageLimitError(subtype)
                    if message.is_error:
                        # Other result errors: surface the subtype for visibility.
                        logger.warning(
                            "Claude result error (subtype=%s, stop_reason=%s)",
                            message.subtype,
                            message.stop_reason,
                        )
                    # When output_format=json_schema is used, the SDK puts the
                    # validated object in `structured_output` and `result` is
                    # empty / a status string ("structured output submitted…").
                    # Serialize structured_output back to JSON text so callers
                    # that json.loads() the return keep working. Fall back to the
                    # plain text result for non-schema calls.
                    structured = getattr(message, "structured_output", None)
                    if structured is not None:
                        result = json.dumps(structured, ensure_ascii=False)
                    else:
                        result = message.result

        try:
            # Keep collection in this task. `asyncio.wait_for(_collect(), ...)`
            # runs `_collect()` in a child task, but the Claude SDK client must
            # be disconnected by the same task that consumed it.
            async with asyncio.timeout(timeout):
                await _collect()
        except ClaudeUsageLimitError as e:
            logger.error("Claude usage limit reached (subtype=%s)", e.subtype)
            await _disconnect(
                reason="usage-limit handling",
                propagate_cancel=False,
            )
            raise
        except asyncio.TimeoutError:
            logger.warning(
                "Claude call timed out after %ds (use_web=%s), disconnecting client",
                timeout,
                use_web,
            )
            await _disconnect(
                reason="timeout handling",
                propagate_cancel=False,
            )
            raise ClaudeTimeoutError(
                f"Claude call timed out after {timeout}s (use_web={use_web})"
            )
        except asyncio.CancelledError:
            # If our own task is cancelled externally, clean up the client too
            try:
                await _disconnect("task cancellation")
            except asyncio.CancelledError:
                pass
            raise
        finally:
            await _disconnect("normal completion", propagate_cancel=False)

        return result


@with_retry(
    max_attempts=2,
    base_delay=2.0,
    exceptions=(CLIConnectionError, ProcessError, ClaudeTimeoutError),
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
    max_attempts=3,
    base_delay=2.0,
    exceptions=(CLIConnectionError, ProcessError, ClaudeTimeoutError),
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
    max_attempts=3,
    base_delay=2.0,
    exceptions=(CLIConnectionError, ProcessError, ClaudeTimeoutError),
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
    match = re.search(
        r'https?://(?:[a-z]{2,3}\.)?linkedin\.com/in/[^\s\)"\]\',]+', text
    )
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
    max_attempts=3,
    base_delay=2.0,
    exceptions=(CLIConnectionError, ProcessError, ClaudeTimeoutError),
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
    max_attempts=3,
    base_delay=2.0,
    exceptions=(CLIConnectionError, ProcessError, ClaudeTimeoutError),
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
    max_attempts=2,
    base_delay=2.0,
    exceptions=(CLIConnectionError, ProcessError, ClaudeTimeoutError),
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
    max_attempts=2,
    base_delay=2.0,
    exceptions=(CLIConnectionError, ProcessError, ClaudeTimeoutError),
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
        emp_match = re.search(
            r"(\d[\d,.']+)\s*(?:employees|Mitarbeiter|staff|people)", result, re.I
        )
        if emp_match:
            data["employees_count"] = emp_match.group(1).replace("'", ",")
        rev_match = re.search(
            r"(?:revenue|turnover|Umsatz)[:\s]*([€$£]\s*[\d.,]+\s*[BMKbmk]?(?:illion)?)",
            result,
            re.I,
        )
        if rev_match:
            data["revenue"] = rev_match.group(1).strip()
        if data:
            logger.info("Extracted financials from text: %s", data)
            return data
        logger.warning("Could not extract financials from response: %s", result[:200])

    return {}


@with_retry(
    max_attempts=2,
    base_delay=2.0,
    exceptions=(CLIConnectionError, ProcessError, ClaudeTimeoutError),
)
async def summarize_business(
    company_name: str,
    country: str | None = None,
    legal_form: str | None = None,
) -> dict:
    """Web-search call #1: a short business description only.

    Deliberately narrow so it finishes within the turn budget. Returns
    {"business_description": str|None, "raw": str}.
    """
    location = f" The company is based in {country}." if country else ""
    form = f" Legal form: {legal_form}." if legal_form else ""

    prompt = f"""Research the company "{company_name}".{location}{form}

Write a 2-4 sentence professional business description: what the company does,
its industry/sector, and its scale. Suitable for a business research report.
Only state things you can verify from search results; if you find very little,
write a brief description based on the name and any available information.

Respond with ONLY a JSON object (no markdown): {{"business_description": "..."}}"""

    result = await _ask_claude(
        prompt=prompt,
        use_web=True,
        output_format={
            "type": "json_schema",
            "schema": {
                "type": "object",
                "properties": {
                    "business_description": {"type": ["string", "null"]},
                },
                "required": ["business_description"],
                "additionalProperties": False,
            },
        },
    )

    raw = result or ""
    parsed = _try_parse_json(result)
    if isinstance(parsed, dict) and parsed.get("business_description"):
        return {"business_description": parsed["business_description"], "raw": raw}

    # Fallback: salvage prose from a non-JSON response.
    if raw.strip():
        salvaged = _strip_markdown(raw)
        if len(salvaged) > 1000:
            salvaged = salvaged[:997] + "..."
        return {"business_description": salvaged or None, "raw": raw}

    logger.warning(
        "summarize_business: empty/unparseable response for '%s' (len=%d)",
        company_name,
        len(raw),
    )
    return {"business_description": None, "raw": raw}


@with_retry(
    max_attempts=2,
    base_delay=2.0,
    exceptions=(CLIConnectionError, ProcessError, ClaudeTimeoutError),
)
async def research_leadership(
    company_name: str,
    country: str | None = None,
    legal_form: str | None = None,
    known_ceo_name: str | None = None,
    known_ceo_title: str | None = None,
) -> dict:
    """Web-search call #2: the CEO / legal representative only.

    Narrow so it finishes within the turn budget. Returns
    {"name", "title", "linkedin_url", "career_summary", "raw"}.
    """
    location = f" The company is based in {country}." if country else ""

    kg_hint = ""
    if legal_form and "co. kg" in legal_form.lower():
        kg_hint = (
            " This is a GmbH & Co. KG — the actual Geschäftsführer is typically "
            "the managing director of the Komplementär-GmbH (general partner)."
        )

    if known_ceo_name:
        task = (
            f'The known managing director / CEO is "{known_ceo_name}"'
            f'{f" ({known_ceo_title})" if known_ceo_title else ""}. Confirm this '
            "person, find their LinkedIn profile URL, and write a 2-3 sentence "
            "career summary."
        )
    else:
        task = (
            "Find the current CEO, Geschäftsführer, or managing director: their "
            "name, title, LinkedIn profile URL, and a 2-3 sentence career summary."
        )

    prompt = f"""Research the leadership of "{company_name}".{location}{kg_hint}

{task}

Use null for anything you cannot verify. Do not guess.
Respond with ONLY a JSON object (no markdown):
{{"name": "..." or null, "title": "..." or null, "linkedin_url": "..." or null, "career_summary": "..." or null}}"""

    result = await _ask_claude(
        prompt=prompt,
        use_web=True,
        output_format={
            "type": "json_schema",
            "schema": {
                "type": "object",
                "properties": {
                    "name": {"type": ["string", "null"]},
                    "title": {"type": ["string", "null"]},
                    "linkedin_url": {"type": ["string", "null"]},
                    "career_summary": {"type": ["string", "null"]},
                },
                "required": ["name", "title", "linkedin_url", "career_summary"],
                "additionalProperties": False,
            },
        },
    )

    raw = result or ""
    parsed = _try_parse_json(result)
    if isinstance(parsed, dict):
        return {
            "name": parsed.get("name"),
            "title": parsed.get("title"),
            "linkedin_url": parsed.get("linkedin_url"),
            "career_summary": parsed.get("career_summary"),
            "raw": raw,
        }

    # Fallback: at least try to pull a LinkedIn URL from prose.
    if not raw.strip():
        logger.warning(
            "research_leadership: empty/unparseable response for '%s' (len=%d)",
            company_name,
            len(raw),
        )
    return {
        "name": None,
        "title": None,
        "linkedin_url": _extract_linkedin_url(raw) if raw else None,
        "career_summary": None,
        "raw": raw,
    }


async def enrich_company(
    company_name: str,
    country: str | None = None,
    legal_form: str | None = None,
    known_ceo_name: str | None = None,
    known_ceo_title: str | None = None,
    known_revenue: str | None = None,  # kept for call-site compatibility; unused
    known_employees: str | None = None,  # kept for call-site compatibility; unused
) -> dict:
    """Orchestrate the two focused web calls: business summary + leadership.

    Split out of a single mega-call because, at high effort, one call asking for
    leadership + financials + description launched too many web searches and ran
    out of turns before answering (empty result). Each focused call finishes
    within budget. Financials are NO LONGER requested from Claude — Northdata is
    the source of truth for those.

    Returns the same shape callers already use::

        {
          "ceo": {"name", "title", "linkedin_url", "career_summary"},
          "business_description": str | None,
          "raw": str,            # combined raw text from both calls
        }
    """
    summary = await summarize_business(
        company_name=company_name,
        country=country,
        legal_form=legal_form,
    )
    leadership = await research_leadership(
        company_name=company_name,
        country=country,
        legal_form=legal_form,
        known_ceo_name=known_ceo_name,
        known_ceo_title=known_ceo_title,
    )

    combined_raw = "\n\n".join(
        part
        for part in (
            f"[business]\n{summary.get('raw', '')}".strip(),
            f"[leadership]\n{leadership.get('raw', '')}".strip(),
        )
        if part
    )

    return {
        "ceo": {
            "name": leadership.get("name"),
            "title": leadership.get("title"),
            "linkedin_url": leadership.get("linkedin_url"),
            "career_summary": leadership.get("career_summary"),
        },
        "business_description": summary.get("business_description"),
        "raw": combined_raw,
    }


@with_retry(
    max_attempts=2,
    base_delay=2.0,
    exceptions=(CLIConnectionError, ProcessError, ClaudeTimeoutError),
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
        match = re.search(
            r"(\d[\d,.']+)\s*(?:employees|Mitarbeiter|staff|people)", result, re.I
        )
        if match:
            return match.group(1).replace("'", ",")

    return None


@with_retry(
    max_attempts=2,
    base_delay=2.0,
    exceptions=(CLIConnectionError, ProcessError, ClaudeTimeoutError),
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
        facts.append(
            f"CEO/Managing Director: {ceo_name} ({ceo_title or 'unknown title'})"
        )

    if related_entities:
        for entity in related_entities:
            parts = [
                f"Related entity: {entity.get('name', '?')} (role: {entity.get('role', '?')})"
            ]
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
