"""Generic website scraper for company /team /about pages to find CEO information."""

from __future__ import annotations

import logging
import re

import httpx
from bs4 import BeautifulSoup

from utils.retry import with_retry

logger = logging.getLogger(__name__)

# Paths to try on company websites
TEAM_PATHS = [
    "/team", "/about", "/about-us", "/management", "/leadership",
    "/ueber-uns", "/unternehmen", "/geschaeftsfuehrung",
    "/equipe", "/direction", "/qui-sommes-nous",
    "/over-ons", "/bestuur",
]

# Title patterns that indicate CEO/leader
CEO_PATTERNS = [
    r"CEO", r"Chief Executive", r"Geschäftsführer", r"Geschaeftsfuehrer",
    r"Managing Director", r"Directeur (?:Général|General)",
    r"Gérant", r"Bestuurder", r"Amministratore Delegato",
    r"General Manager", r"President", r"Founder.*CEO",
    r"CEO.*Founder", r"Inhaber",
]

CEO_RE = re.compile("|".join(CEO_PATTERNS), re.IGNORECASE)


@with_retry(max_attempts=2, base_delay=1.0, exceptions=(httpx.HTTPError, httpx.TimeoutException))
async def find_ceo_from_website(company_name: str, website_url: str | None = None) -> dict | None:
    """
    Try to find CEO/leader information from a company website.

    Args:
        company_name: Company name (used for search if no URL provided)
        website_url: Direct company website URL (optional)

    Returns dict with keys: name, title, source_url, or None if not found.
    """
    if not website_url:
        return None

    # Normalize URL
    base_url = website_url.rstrip("/")
    if not base_url.startswith("http"):
        base_url = f"https://{base_url}"

    async with httpx.AsyncClient(
        timeout=15.0,
        follow_redirects=True,
        headers={"User-Agent": "Mozilla/5.0 (compatible; research-bot/1.0)"},
    ) as client:
        # Try each team/about page path
        for path in TEAM_PATHS:
            url = f"{base_url}{path}"
            try:
                resp = await client.get(url)
                if resp.status_code != 200:
                    continue

                result = _extract_ceo_from_html(resp.text, url)
                if result:
                    logger.debug("Found CEO on %s: %s", url, result["name"])
                    return result
            except Exception:
                continue

        # Try the homepage as last resort
        try:
            resp = await client.get(base_url)
            if resp.status_code == 200:
                result = _extract_ceo_from_html(resp.text, base_url)
                if result:
                    return result
        except Exception:
            pass

    return None


def _extract_ceo_from_html(html: str, url: str) -> dict | None:
    """Extract CEO/leader info from an HTML page."""
    soup = BeautifulSoup(html, "html.parser")

    # Remove scripts, styles, nav, footer
    for tag in soup.find_all(["script", "style", "nav", "footer", "header"]):
        tag.decompose()

    text = soup.get_text(separator="\n")
    lines = [l.strip() for l in text.split("\n") if l.strip()]

    # Strategy 1: Find lines with CEO-related titles and nearby name-like text
    for i, line in enumerate(lines):
        if CEO_RE.search(line):
            # Check this line and adjacent lines for a name
            name = _extract_name_near(lines, i)
            if name:
                title = _extract_title(line)
                return {
                    "name": name,
                    "title": title or "CEO",
                    "source_url": url,
                }

    # Strategy 2: Look for structured elements (cards, list items) with CEO titles
    for el in soup.find_all(["div", "li", "article", "section"], class_=True):
        el_text = el.get_text(separator="\n")
        if CEO_RE.search(el_text):
            el_lines = [l.strip() for l in el_text.split("\n") if l.strip()]
            for j, el_line in enumerate(el_lines):
                if CEO_RE.search(el_line):
                    name = _extract_name_near(el_lines, j)
                    if name:
                        return {
                            "name": name,
                            "title": _extract_title(el_line) or "CEO",
                            "source_url": url,
                        }

    return None


def _extract_name_near(lines: list[str], index: int) -> str | None:
    """Extract a person's name from lines near a title mention."""
    # Check the title line itself for "Name - Title" or "Title: Name" patterns
    line = lines[index]

    # "Name, Title" or "Name - Title"
    match = re.match(
        r"([A-ZÀ-Ü][a-zà-ü]+(?:\s+[A-ZÀ-Ü][a-zà-ü]+){1,3})\s*[,\-–|]\s*.*(?:"
        + "|".join(CEO_PATTERNS) + ")",
        line,
    )
    if match:
        return match.group(1).strip()

    # "Title: Name" or "Title - Name"
    match = re.search(
        r"(?:" + "|".join(CEO_PATTERNS) + r")\s*[:\-–|]\s*([A-ZÀ-Ü][a-zà-ü]+(?:\s+[A-ZÀ-Ü][a-zà-ü]+){1,3})",
        line,
    )
    if match:
        return match.group(1).strip()

    # Check adjacent lines (above and below)
    for offset in [-1, 1, -2, 2]:
        adj_idx = index + offset
        if 0 <= adj_idx < len(lines):
            adj = lines[adj_idx]
            # Is this line a plausible name? (2-4 capitalized words, no special chars)
            if re.match(r"^[A-ZÀ-Ü][a-zà-ü]+(?:\s+[A-ZÀ-Ü][a-zà-ü]+){1,3}$", adj):
                if len(adj) < 50:
                    return adj

    return None


def _extract_title(line: str) -> str | None:
    """Extract the role title from a line containing a CEO pattern match."""
    match = CEO_RE.search(line)
    if match:
        # Return a reasonable title string
        # Try to get more context around the match
        start = max(0, match.start() - 10)
        end = min(len(line), match.end() + 20)
        title_fragment = line[start:end].strip()
        # Clean up
        title_fragment = re.sub(r"^[,\-–|:\s]+", "", title_fragment)
        title_fragment = re.sub(r"[,\-–|:\s]+$", "", title_fragment)
        return title_fragment if len(title_fragment) < 80 else match.group(0)
    return None
