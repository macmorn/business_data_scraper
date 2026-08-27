"""Hibernate-and-retry wrapper for Claude usage limits.

When a Claude-using step hits the usage limit it raises
`ClaudeUsageLimitError` (after parking its remaining work back to a pending
stage, so re-running resumes where it left off). Rather than exiting and making
the operator restart manually, `run_with_hibernation` catches that error, sleeps
for a fixed interval, and re-invokes the step — looping until the step completes
without hitting the limit.

This relies on the stages being resumable: each Claude stage pulls its pending
work fresh from the DB on entry, so re-invoking it continues the remaining
companies.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable

from clients.claude_ai import ClaudeUsageLimitError

logger = logging.getLogger(__name__)

DEFAULT_HIBERNATE_SECONDS = 600  # 10 minutes


def _looks_like_claude_cleanup_cancel(error: asyncio.CancelledError) -> bool:
    """True for the Claude SDK async-generator cleanup cancellation leak."""
    text = repr(error)
    return "async_generator_athrow" in text


def _clear_current_task_cancel() -> None:
    """Clear a cancellation request after a known non-user cleanup leak."""
    task = asyncio.current_task()
    if not task:
        return
    while task.cancelling():
        task.uncancel()


async def _sleep_hibernating(seconds: int, *, label: str) -> None:
    """Sleep, ignoring only Claude SDK cleanup cancellations."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + seconds
    cleanup_cancel_seen = False

    while True:
        remaining = deadline - loop.time()
        if remaining <= 0:
            return
        try:
            await asyncio.sleep(remaining)
            return
        except KeyboardInterrupt:
            logger.warning(
                "%s hibernation aborted by user; remaining work stays parked.",
                label,
            )
            raise
        except asyncio.CancelledError as e:
            if _looks_like_claude_cleanup_cancel(e):
                _clear_current_task_cancel()
                if not cleanup_cancel_seen:
                    logger.warning(
                        "%s hibernation sleep was interrupted by Claude SDK "
                        "cleanup cancellation; continuing to hibernate.",
                        label,
                    )
                    cleanup_cancel_seen = True
                continue
            logger.warning(
                "%s hibernation aborted by user; remaining work stays parked.",
                label,
            )
            raise


async def run_with_hibernation(
    step: Callable[[], Awaitable[object]],
    *,
    label: str,
    hibernate_seconds: int = DEFAULT_HIBERNATE_SECONDS,
) -> object:
    """Run an async step, hibernating + retrying whenever it hits the usage limit.

    Args:
        step: zero-arg async callable performing the work (e.g. a stage's run()).
              MUST be resumable — on retry it should continue the remaining work,
              not redo finished work.
        label: human-readable name for log lines (e.g. "Stage 5: AI enrichment").
        hibernate_seconds: how long to sleep between retries (default 600s).

    Returns the step's final return value (from the successful, non-limited run).

    Loops until the step returns without raising ClaudeUsageLimitError. Press
    Ctrl-C (KeyboardInterrupt) to abort during a hibernation sleep.
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            return await step()
        except ClaudeUsageLimitError as e:
            mins = hibernate_seconds // 60
            logger.warning(
                "%s hit the Claude usage limit (subtype=%s) on attempt %d. "
                "Hibernating %d min, then resuming where it left off… "
                "(Ctrl-C to abort)",
                label, e.subtype, attempt, mins,
            )
            await _sleep_hibernating(hibernate_seconds, label=label)
            logger.info("%s resuming after hibernation (attempt %d)…", label, attempt + 1)
