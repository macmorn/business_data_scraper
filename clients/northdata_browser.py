"""Playwright headless browser client for northdata.com company search."""

from __future__ import annotations

import json
import logging
import random
import re

from playwright.async_api import async_playwright, Browser, BrowserContext, Page

logger = logging.getLogger(__name__)

# Northdata URLs that are NOT company pages (used to filter autocomplete results)
_SKIP_HREFS = {
    "help.northdata.com", "_privacy", "_terms", "_contact", "_premium",
    "_data", "_countries", "welcome.northdata.com", "newsletter",
}


async def _human_delay(page: Page, min_ms: int = 300, max_ms: int = 1200) -> None:
    """Wait a randomised amount of time to mimic human pauses."""
    await page.wait_for_timeout(random.randint(min_ms, max_ms))


async def _human_mouse_move(page: Page) -> None:
    """Move the mouse to a random viewport position with realistic curve."""
    vp = page.viewport_size or {"width": 1280, "height": 720}
    x = random.randint(100, vp["width"] - 100)
    y = random.randint(100, vp["height"] - 100)
    steps = random.randint(8, 25)
    await page.mouse.move(x, y, steps=steps)
    await _human_delay(page, 50, 300)


async def _human_scroll(page: Page) -> None:
    """Scroll up or down by a small random amount."""
    delta = random.choice([-120, -80, 80, 120, 200, 300])
    await page.mouse.wheel(0, delta)
    await _human_delay(page, 200, 600)


class NorthdataClient:
    """Headless browser wrapper for searching and scraping northdata.com."""

    def __init__(self, email: str = "", password: str = "", retry_attempts: int = 2):
        self._pw = None
        self._browser: Browser | None = None
        self._context: BrowserContext | None = None
        self._stealth = None
        self._email = email
        self._password = password
        self._retry_attempts = max(1, retry_attempts)
        self._logged_in = False

    async def start(self) -> None:
        """Launch the browser with stealth patching and optional premium login."""
        self._pw = await async_playwright().start()

        # Apply stealth to the browser context (new API)
        try:
            from playwright_stealth import Stealth
            self._stealth = Stealth()
        except ImportError:
            logger.warning("playwright-stealth not installed, proceeding without stealth")
            self._stealth = None

        self._browser = await self._pw.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ],
        )
        self._context = await self._browser.new_context()
        if self._stealth:
            await self._stealth.apply_stealth_async(self._context)

        # Dismiss cookie consent on the homepage once so it persists in the context
        page = await self._context.new_page()
        try:
            await page.goto("https://www.northdata.com/", wait_until="domcontentloaded", timeout=30000)
            await _human_delay(page, 1500, 3000)
            await _human_mouse_move(page)
            accept_btn = await page.query_selector("text=Accept all")
            if accept_btn:
                await accept_btn.click()
                await _human_delay(page, 800, 1500)
                logger.info("Dismissed cookie consent")
        except Exception as e:
            logger.warning("Could not dismiss cookie consent: %s", e)
        finally:
            await page.close()

        # Log in with premium account if credentials provided
        if self._email and self._password:
            await self._login()

        logger.info("Northdata browser launched (logged_in=%s)", self._logged_in)

    async def _login(self) -> None:
        """Log in to Northdata with premium credentials."""
        page = await self._context.new_page()
        try:
            await page.goto("https://www.northdata.com/_login", wait_until="domcontentloaded", timeout=30000)
            await _human_delay(page, 1500, 3000)
            await _human_mouse_move(page)

            # Fill email/username
            email_input = await page.query_selector('input[name="email"], input[type="email"], input[name="username"]')
            if not email_input:
                logger.error("Login page: email input not found")
                return

            await email_input.click()
            await _human_delay(page, 200, 500)
            await page.keyboard.type(self._email, delay=random.randint(20, 60))

            # Fill password
            pw_input = await page.query_selector('input[name="password"], input[type="password"]')
            if not pw_input:
                logger.error("Login page: password input not found")
                return

            await pw_input.click()
            await _human_delay(page, 200, 500)
            await page.keyboard.type(self._password, delay=random.randint(25, 70))
            await _human_delay(page, 300, 800)

            # Submit
            submit_btn = await page.query_selector('button[type="submit"]')
            if submit_btn:
                await _human_mouse_move(page)
                await submit_btn.click()
            else:
                await pw_input.press("Enter")

            await _human_delay(page, 2500, 4000)

            # Verify login succeeded
            content = await page.content()
            if "logout" in content.lower() or "_logout" in content:
                self._logged_in = True
                logger.info("Successfully logged in to Northdata as %s", self._email)
            else:
                logger.warning("Northdata login may have failed — no logout link found")
        except Exception as e:
            logger.error("Northdata login error: %s", e)
        finally:
            await page.close()

    async def stop(self) -> None:
        """Close the browser."""
        if self._context:
            await self._context.close()
        if self._browser:
            await self._browser.close()
        if self._pw:
            await self._pw.stop()
        logger.info("Northdata browser closed")

    async def search(self, company_name: str, search_hint: str | None = None) -> dict:
        """
        Search northdata.com for a company via the homepage autocomplete.
        Retries up to self._retry_attempts times on not_found/error.

        If search_hint (e.g. city name) is provided and the initial search
        returns multiple or not_found, retries once with "{name} {hint}".

        Returns dict with:
            - "status": "found" | "multiple" | "not_found" | "error"
            - "matches": list of match dicts (for "multiple")
            - "data": dict of company data (for "found")
            - "error": error message (for "error")
        """
        last_result = None
        for attempt in range(1, self._retry_attempts + 1):
            result = await self._search_once(company_name)
            last_result = result

            if result["status"] in ("found", "multiple"):
                break

            # Only retry on not_found or transient errors
            if attempt < self._retry_attempts:
                logger.info(
                    "Retry %d/%d for '%s' (was: %s)",
                    attempt, self._retry_attempts, company_name, result["status"],
                )

        # Location-enhanced retry
        if search_hint and last_result["status"] in ("multiple", "not_found"):
            hint_query = f"{company_name} {search_hint}"
            logger.info("Retrying with location hint: '%s'", hint_query)
            hint_result = await self._search_once(hint_query, match_name=company_name)
            if hint_result["status"] in ("found", "multiple"):
                return hint_result

        return last_result

    async def _search_once(self, company_name: str, match_name: str | None = None) -> dict:
        """Single search attempt. If match_name is given, use it for exact-match comparison."""
        page = await self._context.new_page()
        try:
            # Navigate to homepage
            await page.goto("https://www.northdata.com/", wait_until="domcontentloaded", timeout=30000)
            await _human_delay(page, 800, 2000)

            # Simulate human: move mouse around after page load
            await _human_mouse_move(page)
            await _human_delay(page, 200, 600)

            # Check for Cloudflare challenge
            content = await page.content()
            if "challenge-platform" in content or "Just a moment" in content:
                logger.warning("Cloudflare challenge detected for '%s'", company_name)
                return {"status": "error", "error": "cloudflare_challenge"}

            # Occasional scroll before interacting (like a human scanning the page)
            if random.random() < 0.4:
                await _human_scroll(page)
                await _human_delay(page, 300, 800)

            # Type into the search input to trigger autocomplete
            search_input = await page.query_selector('input[name="query"]')
            if not search_input:
                logger.error("Search input not found on Northdata homepage")
                return {"status": "error", "error": "search_input_not_found"}

            # Move mouse toward the search input before clicking
            box = await search_input.bounding_box()
            if box:
                target_x = box["x"] + box["width"] * random.uniform(0.2, 0.8)
                target_y = box["y"] + box["height"] * random.uniform(0.3, 0.7)
                await page.mouse.move(target_x, target_y, steps=random.randint(10, 20))
                await _human_delay(page, 100, 300)

            # Clear any previous text, then type with randomised per-key delay
            await search_input.click()
            await search_input.fill("")
            await _human_delay(page, 150, 400)
            await page.keyboard.type(company_name, delay=random.randint(25, 75))
            # Wait for autocomplete AJAX with jitter
            await page.wait_for_timeout(random.randint(2000, 3500))

            # Parse autocomplete suggestions
            results = await self._parse_autocomplete(page)

            if not results:
                logger.debug("No autocomplete results for '%s'", company_name)
                return {"status": "not_found"}
            elif len(results) == 1:
                data = await self._follow_and_scrape(page, results[0])
                return {"status": "found", "data": data}
            else:
                # Check if the first result is an exact/close match
                name_upper = (match_name or company_name).upper().strip()
                for r in results:
                    if r["name"].upper().strip() == name_upper:
                        data = await self._follow_and_scrape(page, r)
                        return {"status": "found", "data": data}
                return {"status": "multiple", "matches": results}

        except Exception as e:
            logger.error("Northdata search error for '%s': %s", company_name, e)
            return {"status": "error", "error": str(e)}
        finally:
            await page.close()

    async def scrape_company_url(self, url: str) -> dict:
        """Scrape a specific Northdata company page by URL."""
        page = await self._context.new_page()
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(2000)
            return await self._scrape_company_page(page)
        finally:
            await page.close()

    async def _parse_autocomplete(self, page: Page) -> list[dict]:
        """Parse autocomplete dropdown suggestions into structured data."""
        results = []

        # Autocomplete results appear as <a> links anywhere on the page
        links = await page.query_selector_all("a")

        for link in links[:20]:
            try:
                href = await link.get_attribute("href") or ""

                # Skip non-company links (nav, footer, help, etc.)
                if not href or "northdata.com/" not in href:
                    continue
                if any(skip in href for skip in _SKIP_HREFS):
                    continue
                # Must look like a company page URL (contains encoded comma, spaces, or plus signs)
                if "%2C" not in href and "%20" not in href and "+" not in href and "/Companies%20House" not in href:
                    continue

                text = await link.inner_text()
                lines = [line.strip() for line in text.split("\n") if line.strip()]
                if not lines:
                    continue

                # Clean up name (remove dagger/cross symbols for dissolved companies)
                name = lines[0].replace("\u202f", " ").replace("\u2020", "").replace("\u271e", "").strip()

                results.append({
                    "name": name,
                    "url": href,
                    "details": " | ".join(lines[1:]) if len(lines) > 1 else "",
                })
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
            await _human_delay(page, 1000, 2500)
            # Simulate reading the page
            await _human_mouse_move(page)
            if random.random() < 0.5:
                await _human_scroll(page)

        return await self._scrape_company_page(page)

    async def _scrape_company_page(self, page: Page) -> dict:
        """Extract structured data from a Northdata company detail page.

        Parses the structured HTML tables (Financials KPI, Representatives,
        Identification) rather than relying on regex over body text.
        """
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
            # Rich fields
            "register_id": None,
            "register_court": None,
            "lei": None,
            "vat_id": None,
            "revenue": None,
            "earnings": None,
            "total_assets": None,
            "equity": None,
            "equity_ratio": None,
            "employees_count": None,
            "return_on_sales": None,
            "cost_of_materials": None,
            "wages_and_salaries": None,
            "cash_on_hand": None,
            "liabilities": None,
            "pension_provisions": None,
            "auditor": None,
            "financials_json": None,
            "public_funding_total": None,
            "corporate_purpose": None,
            "industry_code": None,
        }

        try:
            # Company name from h1
            el = await page.query_selector("h1")
            if el:
                data["name"] = (await el.inner_text()).strip()

            data["legal_form"] = _extract_legal_form(data["name"] or "")

            # Body text for regex-based extraction
            body_text = await page.inner_text("body")

            # Status from body text
            if re.search(r"\b(gelöscht|dissolved|liquidat|insolv)\b", body_text, re.I):
                data["status"] = "dissolved"
            elif re.search(r"\b(aktiv|active|registered)\b", body_text, re.I):
                data["status"] = "active"

            # Address from the NAME/ID/ADDRESS section
            addr_match = re.search(r"ADDRESS\n(.+?)(?:\n[A-Z]{2,}|\nCORPORATE)", body_text)
            if addr_match:
                data["address"] = addr_match.group(1).strip()

            # Identification section
            self._parse_identification(body_text, data)

            # Corporate purpose / industry code
            purpose_match = re.search(
                r"CORPORATE PURPOSE\n([\d.]+)\n(.+?)(?:\nCONTACT|\nEARNINGS|\nREVENUE|\nHISTORY)",
                body_text, re.S,
            )
            if purpose_match:
                data["industry_code"] = purpose_match.group(1).strip()
                purpose_text = purpose_match.group(2).strip()
                # Truncate very long purposes
                if len(purpose_text) > 500:
                    purpose_text = purpose_text[:497] + "..."
                data["corporate_purpose"] = purpose_text

            # Founded year from HISTORY section — only match explicit founding keywords
            year_match = re.search(r"(?:gegründet|founded|incorporated|Gründung)\s*(?:in)?\s*(\d{4})", body_text, re.I)
            if year_match:
                data["founded_year"] = int(year_match.group(1))
            else:
                # Try: year before founding keyword (e.g. "1908 Gründung")
                year_match = re.search(r"(\d{4})\s+(?:Gründung|founded|incorporated)", body_text, re.I)
                if year_match:
                    data["founded_year"] = int(year_match.group(1))

            # Parse structured tables
            tables = await page.query_selector_all("table")

            for table in tables:
                cls = await table.get_attribute("class") or ""

                # Representatives table
                if "company-representatives" in cls:
                    data["officers"] = await self._parse_representatives_table(table)

                # Financials KPI table (first .bizq table with "Financials" header)
                elif "bizq" in cls:
                    rows = await table.query_selector_all("tr")
                    if rows:
                        first_row = await rows[0].inner_text()
                        if "Financials" in first_row or "Finanzkennzahlen" in first_row:
                            await self._parse_financials_table(rows, data)
                        elif "Mktg" in first_row or "Marketing" in first_row:
                            await self._parse_mktg_table(rows, data)

        except Exception as e:
            logger.error("Error scraping company page %s: %s", page.url, e)

        return data

    def _parse_identification(self, body_text: str, data: dict) -> None:
        """Parse the IDENTIFICATION section from body text."""
        id_section = re.search(r"IDENTIFICATION\n(.+?)(?:\nADDRESS|\nCORPORATE)", body_text, re.S)
        if not id_section:
            return

        section = id_section.group(1)
        lines = [l.strip() for l in section.split("\n") if l.strip()]

        for i, line in enumerate(lines):
            if line == "Ut" and i + 1 < len(lines):
                # Register: "District Court of Neuss HRB 1878"
                reg = lines[i + 1]
                data["register_court"] = reg
                reg_match = re.search(r"(HRB?\s*\d+|Siren\s*\d+|Companies\s*House\s*\w+|KBO\s*[\d.]+)", reg)
                if reg_match:
                    data["register_id"] = reg_match.group(1)
            elif line == "Lei" and i + 1 < len(lines):
                data["lei"] = lines[i + 1]
            elif line == "VAT Id" and i + 1 < len(lines):
                data["vat_id"] = lines[i + 1]

    async def _parse_representatives_table(self, table) -> list[dict]:
        """Parse the company-representatives table into officer list."""
        officers = []
        rows = await table.query_selector_all("tr")
        current_position = ""

        for row in rows:
            cells = await row.query_selector_all("td")
            if len(cells) < 2:
                continue

            position = (await cells[0].inner_text()).strip()
            person = (await cells[1].inner_text()).strip()

            if position:
                current_position = position

            if not person or len(person) < 3:
                continue

            # Check for a link to the person's Northdata page
            person_link = await cells[1].query_selector("a")
            northdata_person_url = None
            if person_link:
                href = await person_link.get_attribute("href") or ""
                if href:
                    northdata_person_url = (
                        f"https://www.northdata.com{href}" if not href.startswith("http") else href
                    )

            # Person format: "Name, City, Country"
            parts = person.split(",")
            name = parts[0].strip()
            location = ", ".join(p.strip() for p in parts[1:]) if len(parts) > 1 else ""

            officer = {
                "role": current_position,
                "name": name,
                "location": location,
            }
            if northdata_person_url:
                officer["northdata_url"] = northdata_person_url
            officers.append(officer)

        return officers

    async def _parse_financials_table(self, rows, data: dict) -> None:
        """Parse the Financials KPI table. Extracts most recent year values + full history."""
        # Parse header row for dates
        header_cells = await rows[0].query_selector_all("td, th")
        dates = []
        for c in header_cells[1:]:  # skip "Financials" label
            dates.append((await c.inner_text()).strip())

        # Map exact row labels to data keys.
        # Uses exact start-of-string matching to avoid "Revenue" matching
        # "Revenue CAGR" or "Revenue per employee".
        _EXACT_KPI = {
            "base/share capital": "base_share_capital",
            "stammkapital": "base_share_capital",
            "total assets": "total_assets",
            "bilanzsumme": "total_assets",
            "earnings": "earnings",
            "revenue": "revenue",
            "umsatz": "revenue",
            "return on sales": "return_on_sales",
            "umsatzrentabilität": "return_on_sales",
            "equity ratio": "equity_ratio",
            "eigenkapitalquote": "equity_ratio",
            "equity": "equity",
            "eigenkapital": "equity",
            "return on equity": "return_on_equity",
            "employee number": "employees_count",
            "mitarbeiterzahl": "employees_count",
            "revenue per employee": "_skip",
            "average salaries per employee": "_skip",
            "earnings cagr": "_skip",
            "revenue cagr": "_skip",
            "cash on hand": "cash_on_hand",
            "kassenbestand": "cash_on_hand",
            "receivables": "receivables",
            "forderungen": "receivables",
            "liabilities": "liabilities",
            "verbindlichkeiten": "liabilities",
            "cost of materials": "cost_of_materials",
            "materialaufwand": "cost_of_materials",
            "wages and salaries": "wages_and_salaries",
            "personalaufwand": "wages_and_salaries",
            "pension provisions": "pension_provisions",
            "pensionsrückstellungen": "pension_provisions",
            "real estate": "real_estate",
            "taxes": "taxes",
            "auditor": "auditor",
            "abschlussprüfer": "auditor",
            "publication date": "_publication_date",
            "source": "_skip",
        }

        history = {}  # date -> {metric: value}
        most_recent_idx = len(dates) - 1  # last column = most recent

        for row in rows[1:]:
            cells = await row.query_selector_all("td, th")
            if len(cells) < 2:
                continue

            label = (await cells[0].inner_text()).strip()
            label_lower = label.lower()

            # Find matching key using exact prefix match
            key = _EXACT_KPI.get(label_lower)
            if key is None:
                # Try prefix matching for labels with extra text
                for pattern, mapped_key in _EXACT_KPI.items():
                    if label_lower.startswith(pattern):
                        key = mapped_key
                        break

            if not key or key == "_skip":
                continue

            # Extract values for each date column
            for ci, cell in enumerate(cells[1:]):
                if ci >= len(dates):
                    break

                raw = (await cell.inner_text()).strip()
                # Clean value: remove annotations like "* via LLM", "German HGB ..."
                val = re.sub(r"\s*\*.*$", "", raw, flags=re.S).strip()
                val = re.sub(r"\s*German HGB.*$", "", val).strip()
                val = re.sub(r"\s*\d+\s*years?$", "", val).strip()
                val = re.sub(r"\s*via LLM$", "", val).strip()

                if val and val != "N/A":
                    date = dates[ci]
                    history.setdefault(date, {})[key] = val

                    # Set most recent value on data dict
                    if ci == most_recent_idx and key != "_publication_date":
                        if key == "auditor":
                            data[key] = val
                        else:
                            data[key] = val

        # Fall back: if most recent column was N/A, try next most recent
        for key in ["revenue", "earnings", "employees_count", "total_assets", "equity"]:
            if not data.get(key):
                for ci in range(len(dates) - 2, -1, -1):
                    date = dates[ci] if ci < len(dates) else None
                    if date and date in history and key in history[date]:
                        data[key] = history[date][key]
                        break

        # Set employees_range from employees_count
        if data.get("employees_count"):
            data["employees_range"] = data["employees_count"]

        # Set revenue_range from revenue
        if data.get("revenue"):
            data["revenue_range"] = data["revenue"]

        # Last accounts year from the most recent date header
        if dates:
            year_match = re.search(r"(\d{4})", dates[-1])
            if year_match:
                data["last_accounts_year"] = int(year_match.group(1))

        # Store full history as JSON
        if history:
            data["financials_json"] = json.dumps(history, ensure_ascii=False)

    async def _parse_mktg_table(self, rows, data: dict) -> None:
        """Parse the Mktg & Tech table for public funding totals."""
        for row in rows[1:]:
            cells = await row.query_selector_all("td, th")
            if len(cells) < 2:
                continue
            label = (await cells[0].inner_text()).strip().lower()
            if "total public funding" in label or "gesamtförderung" in label:
                # Get the most recent non-N/A value
                for cell in reversed(list(cells[1:])):
                    val = (await cell.inner_text()).strip()
                    if val and val != "N/A":
                        data["public_funding_total"] = val
                        break
                break


def _extract_legal_form(name: str) -> str | None:
    """Extract legal form from a company name (may include ', City, Country' suffix)."""
    # Strip trailing location info (e.g. ", Neuss, Germany")
    base = name.split(",")[0].strip() if "," in name else name
    forms = [
        "GmbH & Co. KGaA", "GmbH & Co. KG", "GmbH", "GesmbH", "UG",
        "AG", "KG", "OHG", "e.K.", "SE",
        "S.A.S.", "S.A.R.L.", "SAS", "SARL", "S.A.", "SA", "SCA", "SNC", "Sàrl",
        "B.V.", "BV", "N.V.", "NV", "VOF", "BVBA", "SPRL",
        "S.p.A.", "S.r.l.", "SPA", "SRL",
        "S.L.", "PLC", "Ltd.", "Ltd", "Limited",
    ]
    for form in forms:
        if base.upper().rstrip(". ").endswith(form.upper()):
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
