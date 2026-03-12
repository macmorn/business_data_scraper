"""Simple async rate limiter using token bucket approach."""

from __future__ import annotations

import asyncio
import random
import time


class RateLimiter:
    """Enforces a minimum delay between operations, with optional jitter."""

    def __init__(self, min_delay: float, max_delay: float | None = None):
        self.min_delay = min_delay
        self.max_delay = max_delay or min_delay
        self._last_call = 0.0
        self._lock = asyncio.Lock()

    async def wait(self) -> None:
        """Wait until enough time has passed since the last call."""
        async with self._lock:
            delay = random.uniform(self.min_delay, self.max_delay)
            now = time.monotonic()
            elapsed = now - self._last_call
            if elapsed < delay:
                await asyncio.sleep(delay - elapsed)
            self._last_call = time.monotonic()
