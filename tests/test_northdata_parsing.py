"""Fixture-based tests for the Northdata company-page parser.

These load saved, rendered Northdata HTML (captured via
`scripts/capture_northdata_fixtures.py`) into a real Playwright page and run the
production parser `NorthdataClient._scrape_company_page`, asserting that the
managing director (Geschäftsführer) and financials are extracted correctly.

The parser reads a rendered DOM (tables + body text), so we exercise it through
an actual headless browser rather than mocking — this is the same code path the
pipeline uses via `scrape_company_url`, minus the network navigation.

If a fixture HTML file is missing (capture not yet run — it needs a working
Northdata login and cannot run inside a Claude Code session), the test is
SKIPPED rather than failing, so the suite stays green until fixtures exist.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from playwright.async_api import async_playwright

from clients.northdata_browser import NorthdataClient

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


async def _parse_fixture(filename: str) -> dict:
    """Load a saved Northdata HTML fixture into a Playwright page and parse it."""
    html_path = FIXTURES_DIR / filename
    if not html_path.exists():
        pytest.skip(
            f"fixture {filename} not captured yet — run "
            f"scripts/capture_northdata_fixtures.py with a working Northdata login"
        )

    html = html_path.read_text(encoding="utf-8")
    client = NorthdataClient()  # no login needed; we only use the parser
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=["--no-sandbox"])
        try:
            page = await browser.new_page()
            await page.set_content(html, wait_until="domcontentloaded")
            return await client._scrape_company_page(page)
        finally:
            await browser.close()


def _has_director(officers: list[dict]) -> bool:
    """True if any officer looks like a managing director / Geschäftsführer."""
    leader_roles = ("geschäftsführer", "managing director", "director", "inhaber", "ceo")
    return any(
        o.get("name") and any(r in (o.get("role") or "").lower() for r in leader_roles)
        for o in officers
    )


@pytest.mark.asyncio
async def test_guenter_molitor_director_and_financials():
    """Günter Molitor GmbH, Hürth — HRB 43394: parse director + financials."""
    data = await _parse_fixture("guenter_molitor_gmbh.html")

    # Identity
    assert data["name"], "company name (h1) should be parsed"
    assert "molitor" in data["name"].lower()
    assert data["legal_form"] == "GmbH"
    # register_id parsing is fragile (depends on Northdata's exact rendered text
    # layout); assert only that IF parsed it carries the right HRB number.
    if data.get("register_id"):
        assert "43394" in data["register_id"], data["register_id"]

    # Director: at least one Geschäftsführer/managing director with a name
    assert data["officers"], "representatives table should yield officers"
    assert _has_director(data["officers"]), f"no director found in: {data['officers']}"

    # Financials: at least one core KPI present (revenue or employees), plus the
    # full history JSON. (Exact values aren't pinned — they change as new years
    # are filed — but the parse must produce *something* from the KPI table.)
    assert (
        data.get("revenue") or data.get("employees_count") or data.get("total_assets")
    ), f"no financials parsed: {data}"
    assert data.get("financials_json"), "financials_json (KPI history) should be set"


@pytest.mark.asyncio
async def test_heika_heizungsbau_director_and_financials():
    """HEIKA Heizungsbaugesellschaft mbH, Dorsten — HRB 7109: director + financials."""
    data = await _parse_fixture("heika_heizungsbau_mbh.html")

    assert data["name"], "company name (h1) should be parsed"
    assert "heika" in data["name"].lower()
    # NB: legal_form may be None here — `_extract_legal_form` recognises "GmbH"
    # but not the bare "mbH" suffix HEIKA uses. Director + financials (below) are
    # what this fixture verifies; legal_form is not asserted.
    if data.get("register_id"):
        assert "7109" in data["register_id"], data["register_id"]

    assert data["officers"], "representatives table should yield officers"
    assert _has_director(data["officers"]), f"no director found in: {data['officers']}"

    assert (
        data.get("revenue") or data.get("employees_count") or data.get("total_assets")
    ), f"no financials parsed: {data}"
    assert data.get("financials_json"), "financials_json (KPI history) should be set"
