"""Доступ к очередям через RabbitMQ management HTTP API."""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from typing import Any
from urllib.parse import quote

import httpx

from tests.e2e.client import RABBITMQ_API_URL, RABBITMQ_AUTH

VHOST = quote('/', safe='')
PAYMENTS_EXCHANGE = 'payments'
DEAD_LETTER_QUEUE = 'payments.new.dlq'


def _api(method: str, path: str, **kwargs: Any) -> httpx.Response:
    response = httpx.request(
        method,
        f'{RABBITMQ_API_URL}/api{path}',
        auth=RABBITMQ_AUTH,
        timeout=10.0,
        **kwargs,
    )
    response.raise_for_status()
    return response


def publish_raw(routing_key: str, payload: str, message_id: str) -> None:
    """Опубликовать сообщение в обменник payments в обход outbox."""
    body = _api(
        'POST',
        f'/exchanges/{VHOST}/{PAYMENTS_EXCHANGE}/publish',
        json={
            'properties': {'content_type': 'application/json', 'message_id': message_id},
            'routing_key': routing_key,
            'payload': payload,
            'payload_encoding': 'string',
        },
    ).json()
    assert body['routed'], body


def peek_dead_letters() -> list[dict[str, Any]]:
    """Посмотреть сообщения DLQ, вернув их в очередь."""
    return _api(
        'POST',
        f'/queues/{VHOST}/{DEAD_LETTER_QUEUE}/get',
        json={'count': 1000, 'ackmode': 'ack_requeue_true', 'encoding': 'auto'},
    ).json()


def wait_dead_letter(
    predicate: Callable[[dict[str, Any]], bool],
    timeout: float = 30.0,
) -> dict[str, Any]:
    """Дождаться в DLQ сообщения, для которого predicate истинен.

    :param predicate: условие по сообщению management API (payload, properties)
    :param timeout: секунд на ожидание
    :return: найденное сообщение
    :raises AssertionError: сообщение не появилось
    """
    deadline = time.monotonic() + timeout
    while True:
        for message in peek_dead_letters():
            if predicate(message):
                return message
        if time.monotonic() > deadline:
            raise AssertionError(f'no matching message in {DEAD_LETTER_QUEUE} after {timeout}s')
        time.sleep(0.5)


def payload_json(message: dict[str, Any]) -> dict[str, Any] | None:
    try:
        return json.loads(message['payload'])
    except ValueError:
        return None
