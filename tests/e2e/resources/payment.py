"""Декларативный ресурс платежа."""

from __future__ import annotations

import time
from decimal import Decimal
from http import HTTPStatus
from typing import Any, Self
from uuid import uuid4

from tests.e2e.client import api_url, auth_headers, get_client
from tests.e2e.resources.base import ANY, assert_response
from tests.e2e.resources.webhook_sink import WebhookSink

FINAL_STATUSES = {'succeeded', 'failed'}


class Payment:
    """Платёж, принятый через HTTP API."""

    def __init__(
        self,
        sink: WebhookSink | None = None,
        amount: str = '1500.00',
        currency: str = 'RUB',
        description: str | None = 'e2e payment',
        metadata: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
        id: str | None = None,
    ) -> None:
        self.sink = sink or WebhookSink()
        self.amount = amount
        self.currency = currency
        self.description = description
        self.metadata = metadata if metadata is not None else {'order_id': uuid4().hex[:8]}
        self.idempotency_key = idempotency_key or f'e2e-{uuid4()}'
        # Явный id — для запросов к несуществующему платежу без create().
        self.id = id
        self.status: str | None = None
        self.body: dict[str, Any] = {}

    def payload(self, **overrides: Any) -> dict[str, Any]:
        payload: dict[str, Any] = {
            'amount': self.amount,
            'currency': self.currency,
            'description': self.description,
            'metadata': self.metadata,
            'webhook_url': self.sink.url,
        }
        payload.update(overrides)
        return payload

    def create(
        self,
        *,
        payload: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        status_code: int = HTTPStatus.ACCEPTED,
        expected_headers: dict[str, Any] | None = None,
        expected_body: dict[str, Any] | None = None,
    ) -> Self:
        """POST /payments.

        :param payload: тело вместо собранного из полей ресурса
        :param headers: заголовки вместо X-API-Key + Idempotency-Key
        :param status_code: ожидаемый HTTP-код (успех — ACCEPTED)
        :param expected_headers: ожидания HTTP-заголовков
        :param expected_body: ожидания полей JSON-тела
        :return: self
        """
        request_headers = headers
        if request_headers is None:
            request_headers = {**auth_headers(), 'Idempotency-Key': self.idempotency_key}
        response = get_client().post(
            api_url('/payments'),
            json=payload if payload is not None else self.payload(),
            headers=request_headers,
        )
        body = assert_response(
            response,
            status_code=status_code,
            expected_headers=expected_headers,
            expected_body=expected_body,
            success_body=(
                {'payment_id': ANY, 'status': 'pending', 'created_at': ANY}
                if status_code == HTTPStatus.ACCEPTED
                else None
            ),
        )
        if status_code == HTTPStatus.ACCEPTED:
            self.id = body['payment_id']
            self.status = body['status']
            assert response.headers['location'] == api_url(f'/payments/{self.id}')
        return self

    def get(
        self,
        *,
        headers: dict[str, str] | None = None,
        status_code: int = HTTPStatus.OK,
        expected_headers: dict[str, Any] | None = None,
        expected_body: dict[str, Any] | None = None,
    ) -> Self:
        """GET /payments/{id}.

        :param headers: заголовки вместо X-API-Key
        :param status_code: ожидаемый HTTP-код (успех — OK)
        :param expected_headers: ожидания HTTP-заголовков
        :param expected_body: ожидания полей JSON-тела
        :return: self
        """
        if self.id is None:
            raise RuntimeError('Payment.get() needs an id')
        response = get_client().get(
            api_url(f'/payments/{self.id}'),
            headers=headers if headers is not None else auth_headers(),
        )
        body = assert_response(
            response,
            status_code=status_code,
            expected_headers=expected_headers,
            expected_body=expected_body,
            success_body=(
                {
                    'id': self.id,
                    'currency': self.currency,
                    'description': self.description,
                    'metadata': self.metadata,
                    'idempotency_key': self.idempotency_key,
                    'webhook_url': self.sink.url,
                }
                if status_code == HTTPStatus.OK
                else None
            ),
        )
        if status_code == HTTPStatus.OK:
            assert Decimal(body['amount']) == Decimal(self.amount)
            self.status = body['status']
            self.body = body
        return self

    def wait_processed(self, timeout: float = 30.0) -> Self:
        """Опрашивать GET, пока consumer не выставит конечный статус.

        :param timeout: секунд на ожидание (шлюз отвечает за 2–5 с)
        :return: self
        :raises AssertionError: статус так и остался pending
        """
        deadline = time.monotonic() + timeout
        while True:
            self.get()
            if self.status in FINAL_STATUSES:
                assert self.body['processed_at'] is not None
                return self
            if time.monotonic() > deadline:
                raise AssertionError(f'payment {self.id} is still {self.status} after {timeout}s')
            time.sleep(0.5)
