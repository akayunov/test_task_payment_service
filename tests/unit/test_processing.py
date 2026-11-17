"""Обработка платежа: шлюз, статус, webhook, идемпотентность повторной доставки."""

import asyncio
from uuid import UUID, uuid4

import pytest

from payments.domain.enums import PaymentStatus
from payments.services.processing import PaymentProcessor
from payments.services.webhook import WebhookDeliveryError
from tests.unit.helpers import (
    WEBHOOK_URL,
    FixedGateway,
    RecordingWebhooks,
    create_payment,
    load_payment,
)


@pytest.mark.parametrize(
    ('result', 'event'),
    [(PaymentStatus.SUCCEEDED, 'payment.succeeded'), (PaymentStatus.FAILED, 'payment.failed')],
)
async def test_processes_payment_and_notifies(client, result, event):
    created = await create_payment(client)
    webhooks = RecordingWebhooks()

    await PaymentProcessor(FixedGateway(result), webhooks).process(UUID(created['payment_id']))

    payment = await load_payment(created['payment_id'])
    assert payment.status == result
    assert payment.processed_at is not None
    assert payment.webhook_delivered_at is not None
    [(url, payload)] = webhooks.calls
    assert url == WEBHOOK_URL
    assert payload.event == event
    assert str(payload.payment.id) == created['payment_id']
    assert payload.payment.status == result


async def test_webhook_failure_keeps_status_and_retry_skips_gateway(client):
    created = await create_payment(client)
    payment_id = UUID(created['payment_id'])
    gateway = FixedGateway(PaymentStatus.SUCCEEDED)
    webhooks = RecordingWebhooks(failures=1)
    processor = PaymentProcessor(gateway, webhooks)

    with pytest.raises(WebhookDeliveryError):
        await processor.process(payment_id)
    payment = await load_payment(created['payment_id'])
    assert payment.status == PaymentStatus.SUCCEEDED
    assert payment.webhook_delivered_at is None

    await processor.process(payment_id)

    assert gateway.calls == 1
    assert len(webhooks.calls) == 2
    assert (await load_payment(created['payment_id'])).webhook_delivered_at is not None


async def test_redelivery_after_success_does_nothing(client):
    created = await create_payment(client)
    gateway = FixedGateway(PaymentStatus.SUCCEEDED)
    webhooks = RecordingWebhooks()
    processor = PaymentProcessor(gateway, webhooks)

    await processor.process(UUID(created['payment_id']))
    await processor.process(UUID(created['payment_id']))

    assert gateway.calls == 1
    assert len(webhooks.calls) == 1


async def test_duplicate_messages_in_parallel_charge_once(client):
    """Дубль события в соседнем consumer ждёт блокировку строки и не списывает второй раз."""
    created = await create_payment(client)
    gateway = FixedGateway(PaymentStatus.SUCCEEDED, delay=0.3)
    processor = PaymentProcessor(gateway, RecordingWebhooks())

    await asyncio.gather(
        processor.process(UUID(created['payment_id'])),
        processor.process(UUID(created['payment_id'])),
    )

    assert gateway.calls == 1


async def test_unknown_payment_is_skipped():
    gateway = FixedGateway(PaymentStatus.SUCCEEDED)
    webhooks = RecordingWebhooks()

    await PaymentProcessor(gateway, webhooks).process(uuid4())

    assert gateway.calls == 0
    assert webhooks.calls == []
