"""Playwright headless browser client for northdata.com company search."""

from __future__ import annotations

import json
import logging
import re
from urllib.parse import quote

from playwright.async_api import async_playwright, Browser, Page

logger = logging.getLogger(__name__)


class NorthdataClient:
    """Headless browser wrapper for searching and scraping northdata.com."""

    def __init__(self):
        self._pw = None
        self._browser: Browser | None = None

    async def start(self) -> None:
        """Launch the browser with stealth patching."""
        self._pw = await async_playwright().start()
        # Apply stealth to avoid Cloudflare detection
        try:
            from playwright_stealth import stealth_async
            self._stealth_fn = stealth_async
        except ImportError:
            logger.warning("playwright-stealth not installed, proceeding without stealth")
            self._stealth_fn = None

        self._browser = await self._pw.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ],
        )
        logger.info("Northdata browser launched")

    async def stop(self) -> None:
        """Close the browser."""
        if self._browser:
            await self._browser.close()
        if self._pw:
            await self._pw.stop()
        logger.info("Northdata browser closed")

    async def search(self, company_name: str) -> dict:
        """
        Search northdata.com for a company.

        Returns dict with:
            - "status": "found" | "multiple" | "not_found" | "error"
            - "matches": list of match dicts (for "multiple")
            - "data": dict of company data (for "found")
            - "error": error message (for "error")
        """
        page = await self._browser.new_page()
        try:
            if self._stealth_fn:
                await self._stealth_fn(page)

            # Navigate to search
            search_url = f"https://www.northdata.com/search?q={quote(company_name)}"
            await page.goto(search_url, wait_until="domcontentloaded", timeout=30000)

            # Wait for results to load
            await page.wait_for_timeout(2000)

            # Check what we got
            content = await page.content()

            # Check for Cloudflare challenge
            if "challenge-platform" in content or "Just a moment" in content:
                logger.warning("Cloudflare challenge detected for '%s'", company_name)
                return {"status": "error", "error": "cloudflare_challenge"}

            # Check for no results
            no_results = await page.query_selector_all(".no-results, .empty-results")
            if no_results:
                return {"status": "not_found"}

            # Check if we landed directly on a company page
            if "/company/" in page.url or await page.query_selector(".company-info, .company-header"):
                data = await self._scrape_company_page(page)
                return {"status": "found", "data": data}

            # Check for search results list
            results = await self._parse_search_results(page)
            if not results:
                return {"status": "not_found"}
            elif len(results) == 1:
                # Follow the single result
                data = await self._follow_and_scrape(page, results[0])
                return {"status": "found", "data": data}
            else:
                return {"status": "multiple", "matches": results}

        except Exception as e:
            logger.error("Northdata search error for '%s': %s", company_name, e)
            return {"status": "error", "error": str(e)}
        finally:
            await page.close()

    async def scrape_company_url(self, url: str) -> dict:
        """Scrape a specific Northdata company page by URL."""
        page = await self._browser.new_page()
        try:
            if self._stealth_fn:
                await self._stealth_fn(page)
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(2000)
            return await self._scrape_company_page(page)
        finally:
            await page.close()

    async def _parse_search_results(self, page: Page) -> list[dict]:
        """Parse search results list into structured data."""
        results = []
        # Try multiple possible selectors for result items
        items = await page.query_selector_all(
            ".result-item, .search-result, [data-company], .company-link, a[href*='/company/']"
        )

        for item in items[:20]:  # Cap at 20 results
            try:
                result = {}
                # Get link and name
                link = await item.get_attribute("href")
                if not link:
                    link_el = await item.query_selector("a[href*='/company/']")
                    if link_el:
                        link = await link_el.get_attribute("href")

                text = await item.inner_text()
                lines = [l.strip() for l in text.split("\n") if l.strip()]

                result["name"] = lines[0] if lines else ""
                result["url"] = link or ""
                result["details"] = " | ".join(lines[1:]) if len(lines) > 1 else ""
                results.append(result)
            except Exception:
                continue

        return results

    async def _follow_and_scrape(self, page: Page, result: dict) -> dict:
        """Follow a search result link and scrape the company page."""
        url = result.get("url", "")
        if url and not url.startswith("http"):
            url = f"https://www.northdata.com{url}"

        if url:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(2000)

        return await self._scrape_company_page(page)

    async def _scrape_company_page(self, page: Page) -> dict:
        """Extract structured data from a Northdata company detail page."""
        data = {
            "name": None,
            "legal_form": None,
            "status": None,
            "address": None,
            "founded_year": None,
            "officers": [],
            "employees_range": None,
            "revenue_range": None,
            "last_accounts_year": None,
            "url": page.url,
        }

        try:
            # Company name - try multiple selectors
            for selector in ["h1", ".company-name", ".company-header h1", "[itemprop='name']"]:
                el = await page.query_selector(selector)
                if el:
                    data["name"] = (await el.inner_text()).strip()
                    break

            # Get the full page text for extraction
            body_text = await page.inner_text("body")

            # Legal form - extract from company name or metadata
            data["legal_form"] = _extract_legal_form(data["name"] or "")

            # Status
            if re.search(r"\b(gelöscht|dissolved|liquidat|insolv)\b", body_text, re.I):
                data["status"] = "dissolved"
            elif re.search(r"\b(aktiv|active|registered)\b", body_text, re.I):
                data["status"] = "active"

            # Address - look for address block
            for selector in [".address", "[itemprop='address']", ".company-address"]:
                el = await page.query_selector(selector)
                if el:
                    data["address"] = (await el.inner_text()).strip().replace("\n", ", ")
                    break

            # Founded year
            year_match = re.search(r"(?:gegründet|founded|incorporated)\s*(?:in)?\s*(\d{4})", body_text, re.I)
            if year_match:
                data["founded_year"] = int(year_match.group(1))

            # Officers / Management
            data["officers"] = await self._extract_officers(page, body_text)

            # Employees
            emp_match = re.search(r"(\d+[\s\-–]+\d+)\s*(?:employees|Mitarbeiter|Beschäftigte)", body_text, re.I)
            if emp_match:
                data["employees_range"] = emp_match.group(1)
            else:
                emp_match = re.search(r"(?:employees|Mitarbeiter|Beschäftigte)[:\s]*(\d+[\s\-–]*\d*)", body_text, re.I)
                if emp_match:
                    data["employees_range"] = emp_match.group(1)

            # Revenue
            rev_match = re.search(
                r"(?:revenue|Umsatz|turnover)[:\s]*([\d.,]+\s*(?:Mio|Mrd|k|M|B)?\.?\s*(?:EUR|€|USD)?)",
                body_text, re.I,
            )
            if rev_match:
                data["revenue_range"] = rev_match.group(1).strip()

            # Last accounts year
            acc_match = re.search(r"(?:Jahresabschluss|annual accounts?|filing)\s*(\d{4})", body_text, re.I)
            if acc_match:
                data["last_accounts_year"] = int(acc_match.group(1))

        except Exception as e:
            logger.error("Error scraping company page: %s", e)

        return data

    async def _extract_officers(self, page: Page, body_text: str) -> list[dict]:
        """Extract officers/management from the page."""
        officers = []

        # Try to find management section
        for selector in [
            ".management", ".officers", "#management",
            "section:has-text('Geschäftsführ')", "section:has-text('Management')",
        ]:
            try:
                section = await page.query_selector(selector)
                if section:
                    text = await section.inner_text()
                    for line in text.split("\n"):
                        line = line.strip()
                        if not line or len(line) < 3:
                            continue
                        # Try to parse "Role: Name" or "Name, Role" patterns
                        officer = _parse_officer_line(line)
                        if officer:
                            officers.append(officer)
                    if officers:
                        break
            except Exception:
                continue

        # Fallback: regex on body text for common officer patterns
        if not officers:
            patterns = [
                r"(?:Geschäftsführer|CEO|Managing Director|Directeur|Gérant)[:\s]+([A-ZÀ-Ü][\w\-]+(?:\s+[A-ZÀ-Ü][\w\-]+)+)",
                r"([A-ZÀ-Ü][\w\-]+(?:\s+[A-ZÀ-Ü][\w\-]+)+)\s*[,\-–]\s*(?:Geschäftsführer|CEO|Managing Director)",
            ]
            for pattern in patterns:
                for match in re.finditer(pattern, body_text):
                    name = match.group(1).strip()
                    if 3 < len(name) < 60:
                        officers.append({"name": name, "role": "Geschäftsführer"})

        return officers


def _extract_legal_form(name: str) -> str | None:
    """Extract legal form suffix from a company name."""
    forms = [
        "GmbH & Co. KGaA", "GmbH & Co. KG", "GmbH", "GesmbH", "UG",
        "AG", "KG", "OHG", "e.K.", "SE",
        "S.A.S.", "S.A.R.L.", "SAS", "SARL", "S.A.", "SA", "SCA", "SNC", "Sàrl",
        "B.V.", "BV", "N.V.", "NV", "VOF", "BVBA", "SPRL",
        "S.p.A.", "S.r.l.", "SPA", "SRL",
        "S.L.", "PLC", "Ltd.", "Ltd", "Limited",
    ]
    for form in forms:
        if name.upper().rstrip(". ").endswith(form.upper()):
            return form
    return None


def _parse_officer_line(line: str) -> dict | None:
    """Try to parse a line as an officer entry."""
    # Skip obviously non-officer lines
    if len(line) > 100 or len(line) < 3:
        return None
    if any(skip in line.lower() for skip in ["page", "more", "show", "click", "http"]):
        return None

    # Pattern: "Role: Name" or "Role - Name"
    match = re.match(
        r"(Geschäftsführer|CEO|CFO|COO|CTO|Director|Managing Director|Vorstand|Aufsichtsrat|"
        r"Prokurist|Liquidator|Gesellschafter|Directeur|Gérant|Bestuurder)"
        r"\s*[:\-–]\s*(.+)",
        line, re.I,
    )
    if match:
        return {"role": match.group(1).strip(), "name": match.group(2).strip()}

    # Pattern: "Name, Role"
    match = re.match(
        r"([A-ZÀ-Ü][\w\-]+(?:\s+[A-ZÀ-Ü][\w\-]+)+)\s*[,\-–]\s*"
        r"(Geschäftsführer|CEO|Director|Managing Director|Vorstand|Prokurist)",
        line, re.I,
    )
    if match:
        return {"name": match.group(1).strip(), "role": match.group(2).strip()}

    return None
