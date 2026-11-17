"""Эмуляция шлюза и HTTP-отправка webhook без сети."""

import json
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import httpx
import pytest

from payments.domain.enums import Currency, PaymentStatus
from payments.schemas.events import WebhookPayload
from payments.schemas.payment import PaymentRead
from payments.services import gateway as gateway_module
from payments.services.gateway import EmulatedGateway
from payments.services.webhook import WebhookDeliveryError, WebhookSender


def make_payload(status: PaymentStatus = PaymentStatus.SUCCEEDED) -> WebhookPayload:
    now = datetime.now(UTC)
    payment = PaymentRead(
        id=uuid4(),
        amount=Decimal('10.00'),
        currency=Currency.EUR,
        description=None,
        metadata={},
        status=status,
        idempotency_key='key',
        webhook_url='https://merchant.example.com/hook',
        created_at=now,
        processed_at=now,
    )
    return WebhookPayload.for_payment(payment)


@pytest.mark.parametrize(
    ('success_rate', 'expected'),
    [(1.0, PaymentStatus.SUCCEEDED), (0.0, PaymentStatus.FAILED)],
)
async def test_gateway_outcome_follows_success_rate(monkeypatch, success_rate, expected):
    delays: list[float] = []

    async def fake_sleep(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr(gateway_module.asyncio, 'sleep', fake_sleep)
    gateway = EmulatedGateway(min_delay=2, max_delay=5, success_rate=success_rate)

    assert await gateway.charge(None) == expected  # type: ignore[arg-type]
    assert 2 <= delays[0] <= 5


def test_webhook_payload_requires_final_status():
    with pytest.raises(ValueError):
        make_payload(PaymentStatus.PENDING)


async def test_webhook_sender_posts_json():
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    payload = make_payload()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await WebhookSender(client).send('https://merchant.example.com/hook', payload)

    [request] = requests
    assert request.method == 'POST'
    assert request.headers['x-event-type'] == 'payment.succeeded'
    assert request.headers['x-payment-id'] == str(payload.payment.id)
    body = json.loads(request.content)
    assert body['event'] == 'payment.succeeded'
    assert body['payment']['amount'] == '10.00'


@pytest.mark.parametrize(
    'response_or_error',
    [httpx.Response(500), httpx.Response(302), httpx.ConnectError('refused')],
    ids=['server-error', 'redirect', 'network'],
)
async def test_webhook_sender_raises_on_failure(response_or_error):
    def handler(request: httpx.Request) -> httpx.Response:
        if isinstance(response_or_error, Exception):
            raise response_or_error
        return response_or_error

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(WebhookDeliveryError):
            await WebhookSender(client).send('https://merchant.example.com/hook', make_payload())
