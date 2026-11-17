"""E2E: outbox → RabbitMQ → consumer → webhook, retry и DLQ на живом стенде."""

import json
import time
from uuid import uuid4

import pytest

from tests.e2e.resources import rabbitmq
from tests.e2e.resources.payment import Payment
from tests.e2e.resources.webhook_sink import WebhookSink

pytestmark = pytest.mark.e2e

EVENT_BY_STATUS = {'succeeded': 'payment.succeeded', 'failed': 'payment.failed'}


def test_payment_is_processed_and_webhook_delivered():
    sink = WebhookSink()
    payment = Payment(sink=sink).create().wait_processed()

    [call] = sink.wait_calls(1)

    assert call['headers']['x-payment-id'] == payment.id
    assert call['body']['event'] == EVENT_BY_STATUS[payment.status]
    assert call['body']['payment']['id'] == payment.id
    assert call['body']['payment']['status'] == payment.status
    assert call['body']['payment']['processed_at'] == payment.body['processed_at']


def test_failing_webhook_is_retried_three_times_then_dead_lettered():
    sink = WebhookSink(failing=True)
    payment = Payment(sink=sink).create().wait_processed()

    # Попытки: сразу, через 2 с, через 4 с — затем DLQ.
    calls = sink.wait_calls(3, timeout=40)
    message = rabbitmq.wait_dead_letter(
        lambda item: (rabbitmq.payload_json(item) or {}).get('payment_id') == payment.id,
    )

    assert len(calls) == 3
    headers = message['properties']['headers']
    assert headers['x-attempt'] == 3
    assert 'HTTP 500' in headers['x-last-error']
    # Статус платежа остался конечным: повторялась только доставка webhook.
    assert payment.get().status in EVENT_BY_STATUS
    time.sleep(3)
    assert len(sink.calls()) == 3


def test_unparsable_message_is_rejected_to_dead_letter_queue():
    message_id = f'e2e-poison-{uuid4()}'
    rabbitmq.publish_raw('payments.new', json.dumps({'unexpected': True}), message_id)

    message = rabbitmq.wait_dead_letter(
        lambda item: item['properties'].get('message_id') == message_id,
        timeout=15,
    )

    # Отклонение брокером по DLX, а не повторная публикация consumer-ом.
    assert message['properties']['headers']['x-death'][0]['reason'] == 'rejected'
