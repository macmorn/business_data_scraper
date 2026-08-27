"""Exponential backoff retry decorator for async functions."""

from __future__ import annotations

import asyncio
import logging
import random
from functools import wraps
from typing import Type

logger = logging.getLogger(__name__)


def with_retry(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    exceptions: tuple[Type[Exception], ...] = (Exception,),
):
    """Decorator: retry an async function with exponential backoff + jitter."""

    def decorator(fn):
        @wraps(fn)
        async def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return await fn(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_attempts - 1:
                        raise
                    delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
                    logger.warning(
                        "%s attempt %d/%d failed: %s. Retrying in %.1fs",
                        fn.__name__, attempt + 1, max_attempts, e, delay,
                    )
                    await asyncio.sleep(delay)

        return wrapper

    return decorator
