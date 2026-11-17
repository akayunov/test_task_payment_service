import asyncio
from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

from fastapi import status
from faststream.rabbit import RabbitExchange
from httpx import AsyncClient, Response
from sqlalchemy import select

from payments.core.config import get_settings
from payments.db.models import OutboxMessage, Payment
from payments.db.session import session_scope
from payments.domain.enums import PaymentStatus
from payments.schemas.events import WebhookPayload
from payments.services.webhook import WebhookDeliveryError

API_PREFIX = get_settings().api_prefix
AUTH = {'X-API-Key': get_settings().api_key}
WEBHOOK_URL = 'https://merchant.example.com/webhooks/payments'


def payment_body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        'amount': '1500.00',
        'currency': 'RUB',
        'description': 'Order #42',
        'metadata': {'order_id': 42},
        'webhook_url': WEBHOOK_URL,
    }
    body.update(overrides)
    return body


async def post_payment(
    client: AsyncClient,
    body: dict[str, Any] | None = None,
    *,
    idempotency_key: str | None = None,
    headers: dict[str, str] | None = None,
) -> Response:
    request_headers = {**AUTH, 'Idempotency-Key': idempotency_key or str(uuid4())}
    if headers is not None:
        request_headers = headers
    return await client.post(
        f'{API_PREFIX}/payments',
        json=body if body is not None else payment_body(),
        headers=request_headers,
    )


async def create_payment(client: AsyncClient, **overrides: Any) -> dict[str, Any]:
    response = await post_payment(client, payment_body(**overrides))
    assert response.status_code == status.HTTP_202_ACCEPTED, response.text
    return response.json()


async def load_payment(payment_id: str) -> Payment:
    async with session_scope() as session:
        payment = await session.get(Payment, payment_id)
        assert payment is not None
        return payment


async def outbox_rows() -> list[OutboxMessage]:
    async with session_scope() as session:
        result = await session.execute(select(OutboxMessage).order_by(OutboxMessage.created_at))
        return list(result.scalars().all())


@dataclass
class Published:
    body: bytes
    exchange: RabbitExchange
    routing_key: str
    message_id: str
    headers: dict[str, Any] | None


@dataclass
class FakePublisher:
    """Запоминает публикации. fail_on — номера вызовов (с 1), которые падают."""

    fail_on: set[int] = field(default_factory=set)
    messages: list[Published] = field(default_factory=list)
    calls: int = 0

    async def publish(
        self,
        body: bytes,
        *,
        exchange: RabbitExchange,
        routing_key: str,
        message_id: str,
        headers: dict[str, Any] | None = None,
    ) -> None:
        self.calls += 1
        if self.calls in self.fail_on:
            raise ConnectionError('broker unavailable')
        self.messages.append(Published(body, exchange, routing_key, message_id, headers))


@dataclass
class FixedGateway:
    result: PaymentStatus
    delay: float = 0.0
    calls: int = 0

    async def charge(self, payment: Payment) -> PaymentStatus:
        del payment
        self.calls += 1
        await asyncio.sleep(self.delay)
        return self.result


@dataclass
class RecordingWebhooks:
    """Первые failures вызовов отвечают ошибкой доставки."""

    failures: int = 0
    calls: list[tuple[str, WebhookPayload]] = field(default_factory=list)

    async def send(self, url: str, payload: WebhookPayload) -> None:
        self.calls.append((url, payload))
        if self.failures > 0:
            self.failures -= 1
            raise WebhookDeliveryError('Webhook endpoint responded with HTTP 500')
