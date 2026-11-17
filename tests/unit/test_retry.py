"""Retry с экспоненциальной задержкой и DLQ после последней попытки."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from payments.messaging.handler import PaymentEventHandler
from payments.messaging.retry import RetryPolicy, attempt_from_headers
from payments.messaging.topology import DEAD_LETTER_EXCHANGE, RETRY_EXCHANGE
from payments.schemas.events import PaymentCreatedEvent
from tests.unit.helpers import FakePublisher

POLICY = RetryPolicy(max_attempts=3, base_delay=2)


class FailingProcessor:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.calls = 0

    async def process(self, payment_id) -> None:
        del payment_id
        self.calls += 1
        if self.error is not None:
            raise self.error


def make_event() -> tuple[PaymentCreatedEvent, bytes]:
    event = PaymentCreatedEvent(event_id=uuid4(), payment_id=uuid4(), occurred_at=datetime.now(UTC))
    return event, event.model_dump_json().encode()


def make_handler(processor: FailingProcessor, publisher: FakePublisher) -> PaymentEventHandler:
    return PaymentEventHandler(processor, publisher, POLICY)  # type: ignore[arg-type]


def test_policy_delays_grow_exponentially():
    assert [POLICY.delay_ms(attempt) for attempt in (1, 2, 3)] == [2000, 4000, 8000]
    assert [queue.name for queue in POLICY.retry_queues()] == [
        'payments.new.retry.2000ms',
        'payments.new.retry.4000ms',
    ]
    assert POLICY.should_retry(2)
    assert not POLICY.should_retry(3)


def test_retry_queue_returns_message_to_main_exchange_after_ttl():
    queue = POLICY.retry_queues()[0]

    assert queue.arguments is not None
    assert {
        'x-message-ttl': 2000,
        'x-dead-letter-exchange': 'payments',
        'x-dead-letter-routing-key': 'payments.new',
    }.items() <= queue.arguments.items()


@pytest.mark.parametrize(
    ('headers', 'expected'),
    [({}, 1), ({'x-attempt': 2}, 2), ({'x-attempt': 'oops'}, 1), ({'x-attempt': 0}, 1)],
)
def test_attempt_from_headers(headers, expected):
    assert attempt_from_headers(headers) == expected


@pytest.mark.parametrize(
    ('attempt', 'routing_key'),
    [(1, 'payments.new.retry.2000ms'), (2, 'payments.new.retry.4000ms')],
)
async def test_failure_goes_to_next_retry_queue(attempt, routing_key):
    event, body = make_event()
    publisher = FakePublisher()
    handler = make_handler(FailingProcessor(RuntimeError('webhook down')), publisher)

    await handler.handle(event, body=body, attempt=attempt, message_id='msg-1')

    [message] = publisher.messages
    assert message.exchange == RETRY_EXCHANGE
    assert message.routing_key == routing_key
    assert message.body == body
    assert message.message_id == 'msg-1'
    assert message.headers == {
        'x-attempt': attempt + 1,
        'x-last-error': 'RuntimeError: webhook down',
    }


async def test_last_failed_attempt_goes_to_dead_letter_queue():
    event, body = make_event()
    publisher = FakePublisher()
    handler = make_handler(FailingProcessor(RuntimeError('webhook down')), publisher)

    await handler.handle(event, body=body, attempt=3, message_id='msg-1')

    [message] = publisher.messages
    assert message.exchange == DEAD_LETTER_EXCHANGE
    assert message.routing_key == 'payments.new'
    assert message.body == body
    assert message.headers == {'x-attempt': 3, 'x-last-error': 'RuntimeError: webhook down'}


async def test_success_publishes_nothing():
    event, body = make_event()
    publisher = FakePublisher()
    processor = FailingProcessor()

    await make_handler(processor, publisher).handle(event, body=body, attempt=1, message_id='m')

    assert processor.calls == 1
    assert publisher.messages == []


async def test_failed_retry_publish_propagates_for_broker_reject():
    """Если повтор опубликовать не удалось, FastStream отклонит сообщение в DLQ."""
    event, body = make_event()
    handler = make_handler(FailingProcessor(RuntimeError('boom')), FakePublisher(fail_on={1}))

    with pytest.raises(ConnectionError):
        await handler.handle(event, body=body, attempt=1, message_id='m')
