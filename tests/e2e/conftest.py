"""Фикстуры e2e: только HTTP к живому стенду, без доступа к БД."""

from __future__ import annotations

import os

import pytest

from tests.e2e.client import DEFAULT_BASE_URL, reset_client, set_client


@pytest.fixture(autouse=True)
def e2e_http_client():
    """Синхронный httpx-клиент на E2E_BASE_URL для декларативных ресурсов."""
    import httpx

    base_url = os.environ.get('E2E_BASE_URL', DEFAULT_BASE_URL)
    with httpx.Client(base_url=base_url, timeout=30.0) as client:
        token = set_client(client)
        try:
            yield client
        finally:
            reset_client(token)
