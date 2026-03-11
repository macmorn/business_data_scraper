"""Generic website scraper for company /team, /about, /management pages.

Used as a fallback to identify CEO/leadership when registry data
doesn't include officer information.
"""

from __future__ import annotations

import logging
import re

import httpx
from bs4 import BeautifulSoup

from utils.retry import with_retry

logger = logging.getLogger(__name__)

# Paths to try when looking for team/management pages
TEAM_PATHS = [
    "/team", "/about", "/about-us", "/management", "/leadership",
    "/ueber-uns", "/unternehmen", "/equipe", "/qui-sommes-nous",
    "/about/team", "/about/management", "/company/team",
    "/impressum",
]

# Keywords that indicate a CEO/leader role
CEO_KEYWORDS = [
    "CEO", "Chief Executive", "Geschäftsführer", "Geschäftsfuhrer",
    "Managing Director", "Directeur Général", "Directeur General",
    "Gérant", "Gerant", "President", "Vorstandsvorsitzender",
    "General Manager", "Amministratore Delegato", "Director General",
]

# Pattern to match a person's name near a CEO keyword
NAME_PATTERN = re.compile(
    r"([A-ZÀ-Ü][a-zà-ü]+(?:\s+(?:von|van|de|der|den|di|da|del|le|la))?(?:\s+[A-ZÀ-Ü][a-zà-ü\-]+)+)"
)


@with_retry(max_attempts=2, base_delay=1.0, exceptions=(httpx.HTTPError, httpx.TimeoutException))
async def fetch_page(url: str) -> str | None:
    """Fetch a web page and return its text content."""
    async with httpx.AsyncClient(
        timeout=15.0,
        follow_redirects=True,
        headers={"User-Agent": "Mozilla/5.0 (compatible; company-research/1.0)"},
    ) as client:
        resp = await client.get(url)
        if resp.status_code != 200:
            return None
        return resp.text


async def find_ceo_from_website(company_name: str, website_url: str | None = None) -> dict | None:
    """Try to find CEO information from a company's website.

    Args:
        company_name: Company name for context
        website_url: Base URL of the company website. If None, search is skipped.

    Returns:
        Dict with 'name', 'title', 'source_url' or None if not found.
    """
    if not website_url:
        return None

    # Normalize base URL
    base = website_url.rstrip("/")
    if not base.startswith("http"):
        base = f"https://{base}"

    for path in TEAM_PATHS:
        url = f"{base}{path}"
        try:
            html = await fetch_page(url)
            if not html:
                continue

            result = _extract_ceo_from_html(html, url)
            if result:
                logger.info("Found CEO for '%s' at %s: %s", company_name, url, result["name"])
                return result

        except Exception as e:
            logger.debug("Failed to fetch %s: %s", url, e)
            continue

    return None


def _extract_ceo_from_html(html: str, source_url: str) -> dict | None:
    """Extract CEO name and title from HTML content."""
    soup = BeautifulSoup(html, "html.parser")

    # Remove script and style elements
    for tag in soup(["script", "style", "nav", "footer", "header"]):
        tag.decompose()

    text = soup.get_text(separator="\n")

    # Strategy 1: Look for CEO keywords near names
    for keyword in CEO_KEYWORDS:
        # Pattern: keyword followed by name
        pattern = re.compile(
            rf"({re.escape(keyword)})\s*[:\-–|,]?\s*" + NAME_PATTERN.pattern,
            re.IGNORECASE,
        )
        match = pattern.search(text)
        if match:
            return {
                "name": match.group(2).strip(),
                "title": match.group(1).strip(),
                "source_url": source_url,
            }

        # Pattern: name followed by keyword
        pattern_rev = re.compile(
            NAME_PATTERN.pattern + r"\s*[,\-–|]\s*" + rf"({re.escape(keyword)})",
            re.IGNORECASE,
        )
        match = pattern_rev.search(text)
        if match:
            return {
                "name": match.group(1).strip(),
                "title": match.group(2).strip(),
                "source_url": source_url,
            }

    # Strategy 2: Look in structured HTML (e.g. team cards)
    for card in soup.select(".team-member, .person, .member, [class*='team'], [class*='person']"):
        card_text = card.get_text(separator=" ")
        for keyword in CEO_KEYWORDS[:5]:
            if keyword.lower() in card_text.lower():
                name_match = NAME_PATTERN.search(card_text)
                if name_match:
                    return {
                        "name": name_match.group(1).strip(),
                        "title": keyword,
                        "source_url": source_url,
                    }

    return None
