"""Outbox relay: публикует неопубликованное, помечает только после успеха."""

import asyncio
import json

from payments.messaging.topology import PAYMENTS_EXCHANGE
from payments.services.outbox_relay import OutboxRelay
from tests.unit.helpers import FakePublisher, create_payment, outbox_rows


async def test_publishes_pending_events_once(client):
    payment = await create_payment(client)
    publisher = FakePublisher()
    relay = OutboxRelay(publisher, batch_size=10)

    assert await relay.publish_pending() == 1
    assert await relay.publish_pending() == 0

    [message] = publisher.messages
    [row] = await outbox_rows()
    assert message.exchange == PAYMENTS_EXCHANGE
    assert message.routing_key == 'payments.new'
    assert message.message_id == str(row.id)
    assert json.loads(message.body)['payment_id'] == payment['payment_id']
    assert row.published_at is not None
    assert row.attempts == 0


async def test_failed_publish_keeps_event_for_next_run(client):
    await create_payment(client)
    await create_payment(client)
    publisher = FakePublisher(fail_on={1})
    relay = OutboxRelay(publisher, batch_size=10)

    # Первая ошибка останавливает пачку: второе событие тоже ждёт следующего прохода.
    assert await relay.publish_pending() == 0
    first, second = await outbox_rows()
    assert first.published_at is None
    assert first.attempts == 1
    assert first.last_error == 'ConnectionError: broker unavailable'
    assert second.attempts == 0

    assert await relay.publish_pending() == 2
    assert all(row.published_at is not None for row in await outbox_rows())


async def test_batch_size_limits_one_pass(client):
    for _ in range(3):
        await create_payment(client)
    relay = OutboxRelay(FakePublisher(), batch_size=2)

    assert await relay.publish_pending() == 2
    assert await relay.publish_pending() == 1


async def test_run_loop_drains_outbox_and_stops(client):
    await create_payment(client)
    publisher = FakePublisher()
    stop = asyncio.Event()
    task = asyncio.create_task(OutboxRelay(publisher, batch_size=10).run(0.05, stop))

    for _ in range(50):
        if publisher.messages:
            break
        await asyncio.sleep(0.05)
    stop.set()
    await asyncio.wait_for(task, timeout=5)

    assert len(publisher.messages) == 1
