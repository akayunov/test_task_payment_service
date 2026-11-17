"""Ресурс приёмника webhook (сервис webhook-sink)."""

from __future__ import annotations

import time
from typing import Any
from uuid import uuid4

import httpx

from tests.e2e.client import WEBHOOK_SINK_URL


class WebhookSink:
    """Уникальный адрес на приёмнике. failing=True — приёмник всегда отвечает 500."""

    def __init__(self, failing: bool = False) -> None:
        prefix = 'fail' if failing else 'ok'
        self.name = f'{prefix}-{uuid4().hex}'

    @property
    def url(self) -> str:
        return f'{WEBHOOK_SINK_URL}/hooks/{self.name}'

    def calls(self) -> list[dict[str, Any]]:
        response = httpx.get(self.url, timeout=10.0)
        response.raise_for_status()
        return response.json()['calls']

    def wait_calls(self, count: int, timeout: float = 30.0) -> list[dict[str, Any]]:
        """Дождаться, пока приёмник получит не меньше count вызовов.

        :param count: сколько вызовов ждём
        :param timeout: секунд на ожидание
        :return: все принятые вызовы
        :raises AssertionError: вызовов меньше count к сроку
        """
        deadline = time.monotonic() + timeout
        while True:
            calls = self.calls()
            if len(calls) >= count:
                return calls
            if time.monotonic() > deadline:
                raise AssertionError(f'{self.name}: expected {count} call(s), got {len(calls)}')
            time.sleep(0.5)
