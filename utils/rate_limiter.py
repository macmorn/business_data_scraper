"""Simple async rate limiter using token bucket approach."""

from __future__ import annotations

import asyncio
import time


class RateLimiter:
    """Enforces a minimum delay between operations."""

    def __init__(self, min_delay: float):
        self.min_delay = min_delay
        self._last_call = 0.0
        self._lock = asyncio.Lock()

    async def wait(self) -> None:
        """Wait until enough time has passed since the last call."""
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_call
            if elapsed < self.min_delay:
                await asyncio.sleep(self.min_delay - elapsed)
            self._last_call = time.monotonic()
