"""Capture rendered Northdata company-page HTML into tests/fixtures/.

The Northdata parser (`clients/northdata_browser._scrape_company_page`) reads a
*rendered* Playwright DOM, and the detail pages are JS-rendered and gated behind
a premium login. To get a faithful fixture we therefore drive the real
`NorthdataClient` (with the configured login) to load each URL, then save
`page.content()` — the rendered HTML — to disk. The parsing test later loads
that saved HTML back into a Playwright page via `set_content`.

This script is NOT part of the pipeline. Run it manually, once, with a WORKING
Northdata account configured in `.env`:

    uv run python scripts/capture_northdata_fixtures.py

It cannot run nested inside a Claude Code session (the Agent SDK / bundled CLI
guard) — run it in a normal terminal.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

import config
from clients.northdata_browser import NorthdataClient, NorthdataLoginError

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("capture_fixtures")

FIXTURES_DIR = Path(__file__).resolve().parent.parent / "tests" / "fixtures"

# (fixture filename, Northdata company URL). Two real cologne-area HVAC firms
# with a known managing director + filed financials.
TARGETS = [
    (
        "guenter_molitor_gmbh.html",
        "https://www.northdata.com/G%C3%BCnter%20Molitor%20GmbH,%20H%C3%BCrth/Amtsgericht%20K%C3%B6ln%20HRB%2043394",
    ),
    (
        "heika_heizungsbau_mbh.html",
        "https://www.northdata.com/HEIKA+Heizungsbaugesellschaft+mbH,+Dorsten/Amtsgericht+Gelsenkirchen+HRB+7109",
    ),
]


async def main() -> int:
    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)

    client = NorthdataClient(
        email=config.NORTHDATA_EMAIL,
        password=config.NORTHDATA_PASSWORD,
        retry_attempts=config.NORTHDATA_RETRY_ATTEMPTS,
    )
    try:
        await client.start()
    except NorthdataLoginError as e:
        logger.error(
            "Northdata login FAILED (%s). Cannot capture logged-in fixtures. "
            "Fix NORTHDATA_EMAIL/PASSWORD and retry.", e,
        )
        await client.stop()
        return 1

    if not client._logged_in:
        logger.warning(
            "Proceeding WITHOUT login — captured HTML may lack premium "
            "director/financials data."
        )

    saved = 0
    try:
        # Reuse the client's context so cookies/login carry over.
        for filename, url in TARGETS:
            page = await client._context.new_page()
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                # Give lazy-loaded tables time to render.
                await page.wait_for_timeout(3000)
                html = await page.content()
                out = FIXTURES_DIR / filename
                out.write_text(html, encoding="utf-8")
                logger.info("Saved %s (%d bytes) from %s", out.name, len(html), url)
                saved += 1
            except Exception as e:
                logger.error("Failed to capture %s: %s", url, e)
            finally:
                await page.close()
    finally:
        await client.stop()

    logger.info("Captured %d/%d fixtures into %s", saved, len(TARGETS), FIXTURES_DIR)
    return 0 if saved == len(TARGETS) else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
