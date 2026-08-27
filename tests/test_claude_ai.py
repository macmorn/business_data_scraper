"""Regression tests for Claude SDK usage-limit handling."""

from __future__ import annotations

import asyncio

import pytest
from claude_agent_sdk import AssistantMessage

from clients import claude_ai
from utils import hibernation


class _UsageLimitClient:
    instances: list["_UsageLimitClient"] = []

    def __init__(self, *args, **kwargs) -> None:
        self.connected = False
        self.queried = False
        self.disconnected = False
        self.receive_task: asyncio.Task | None = None
        self.disconnect_task: asyncio.Task | None = None
        self.__class__.instances.append(self)

    async def connect(self) -> None:
        self.connected = True

    async def query(self, prompt: str) -> None:
        self.queried = True

    async def receive_response(self):
        self.receive_task = asyncio.current_task()
        yield AssistantMessage(content=[], model="fake-sonnet", error="rate_limit")

    async def disconnect(self) -> None:
        self.disconnect_task = asyncio.current_task()
        self.disconnected = True


class _CancellingDisconnectUsageLimitClient(_UsageLimitClient):
    instances: list["_CancellingDisconnectUsageLimitClient"] = []

    async def disconnect(self) -> None:
        self.disconnect_task = asyncio.current_task()
        self.disconnected = True
        task = asyncio.current_task()
        assert task is not None
        task.cancel(
            "Cancelled via cancel scope test by "
            "<Task pending name='Task-10' coro=<<async_generator_athrow without __name__>()>>"
        )
        await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_usage_limit_client_is_disconnected_in_receive_task(monkeypatch):
    _UsageLimitClient.instances.clear()
    monkeypatch.setattr(claude_ai, "ClaudeSDKClient", _UsageLimitClient)

    with pytest.raises(claude_ai.ClaudeUsageLimitError):
        await claude_ai._ask_claude("trigger limit", use_web=True)

    client = _UsageLimitClient.instances[0]
    assert client.connected
    assert client.queried
    assert client.disconnected
    assert client.receive_task is not None
    assert client.disconnect_task is client.receive_task


@pytest.mark.asyncio
async def test_usage_limit_disconnect_cancellation_does_not_poison_task(monkeypatch):
    _CancellingDisconnectUsageLimitClient.instances.clear()
    monkeypatch.setattr(
        claude_ai,
        "ClaudeSDKClient",
        _CancellingDisconnectUsageLimitClient,
    )

    with pytest.raises(claude_ai.ClaudeUsageLimitError):
        await claude_ai._ask_claude("trigger cleanup cancel", use_web=True)

    client = _CancellingDisconnectUsageLimitClient.instances[0]
    assert client.disconnected
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_hibernation_continues_after_claude_cleanup_cancel(monkeypatch):
    calls = 0

    async def fake_sleep(delay):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise asyncio.CancelledError(
                "Cancelled via cancel scope test by "
                "<Task pending name='Task-10' coro=<<async_generator_athrow without __name__>()>>"
            )

    monkeypatch.setattr(hibernation.asyncio, "sleep", fake_sleep)

    await hibernation._sleep_hibernating(600, label="Stage 5: AI enrichment")

    assert calls == 2
