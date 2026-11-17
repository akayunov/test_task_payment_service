"""HTTP-клиент для декларативных e2e: ходит в живое приложение по URL."""

from __future__ import annotations

import os
from contextvars import ContextVar

import httpx

_client: ContextVar[httpx.Client] = ContextVar('e2e_http_client')

API_PREFIX = os.environ.get('E2E_API_PREFIX', '/api/v1')
API_KEY = os.environ.get('E2E_API_KEY', 'dev-only-api-key-change-me')
DEFAULT_BASE_URL = os.environ.get('E2E_BASE_URL', 'http://127.0.0.1:8000')
# Один и тот же адрес видят и consumer (шлёт webhook), и тесты (читают принятые вызовы).
WEBHOOK_SINK_URL = os.environ.get('E2E_WEBHOOK_SINK_URL', 'http://127.0.0.1:8081')
RABBITMQ_API_URL = os.environ.get('E2E_RABBITMQ_API_URL', 'http://127.0.0.1:15672')
RABBITMQ_AUTH = (
    os.environ.get('E2E_RABBITMQ_USER', 'guest'),
    os.environ.get('E2E_RABBITMQ_PASSWORD', 'guest'),
)


def get_client() -> httpx.Client:
    """Текущий клиент из фикстуры теста.

    :return: httpx.Client с base_url окружения
    :raises LookupError: клиент ещё не установлен фикстурой
    """
    return _client.get()


def set_client(client: httpx.Client):
    """Привязать клиент к текущему контексту.

    :param client: открытый httpx.Client
    :return: token для reset
    """
    return _client.set(client)


def reset_client(token) -> None:
    """Сбросить клиент контекста.

    :param token: значение из set_client
    """
    _client.reset(token)


def api_url(path: str) -> str:
    """Собрать путь API с префиксом версии.

    :param path: путь вида /payments
    :return: полный путь с префиксом
    """
    if not path.startswith('/'):
        path = f'/{path}'
    return f'{API_PREFIX}{path}'


def auth_headers() -> dict[str, str]:
    return {'X-API-Key': API_KEY}
