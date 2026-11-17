"""Consumer payments.new. Запуск: faststream run payments.workers.consumer:app"""

from typing import Annotated

import httpx
from faststream import AckPolicy, Context, FastStream
from faststream.rabbit import RabbitMessage

from payments.core.config import get_settings
from payments.core.logging import configure_logging, request_id_ctx
from payments.db.session import dispose_engine
from payments.messaging.broker import BrokerPublisher, build_broker
from payments.messaging.handler import PaymentEventHandler
from payments.messaging.retry import RetryPolicy, attempt_from_headers
from payments.messaging.topology import PAYMENTS_EXCHANGE, PAYMENTS_QUEUE, declare_topology
from payments.schemas.events import PaymentCreatedEvent
from payments.services.gateway import EmulatedGateway
from payments.services.processing import PaymentProcessor
from payments.services.webhook import WebhookSender, build_http_client

configure_logging()
settings = get_settings()
retry_policy = RetryPolicy.from_settings(settings)
broker = build_broker(settings)
app = FastStream(broker)


@app.on_startup
async def setup() -> None:
    # Топология до старта подписчика: очереди повторов и DLQ должны существовать
    # к моменту, когда первое сообщение может упасть.
    await broker.connect()
    await declare_topology(broker, retry_policy.retry_queues())
    http_client = build_http_client(settings)
    handler = PaymentEventHandler(
        processor=PaymentProcessor(
            gateway=EmulatedGateway.from_settings(settings),
            webhooks=WebhookSender(http_client),
        ),
        publisher=BrokerPublisher(broker),
        policy=retry_policy,
    )
    app.context.set_global('http_client', http_client)
    app.context.set_global('payment_handler', handler)


@app.after_shutdown
async def teardown() -> None:
    http_client: httpx.AsyncClient = app.context.get('http_client')
    await http_client.aclose()
    await dispose_engine()


# Тело, которое не разбирается в PaymentCreatedEvent, отклоняется и по DLX уходит в DLQ.
@broker.subscriber(PAYMENTS_QUEUE, PAYMENTS_EXCHANGE, ack_policy=AckPolicy.REJECT_ON_ERROR)
async def handle_payment_created(
    event: PaymentCreatedEvent,
    message: RabbitMessage,
    payment_handler: Annotated[PaymentEventHandler, Context()],
) -> None:
    message_id = message.message_id or str(event.event_id)
    token = request_id_ctx.set(message_id)
    try:
        await payment_handler.handle(
            event,
            body=message.body,
            attempt=attempt_from_headers(message.headers),
            message_id=message_id,
        )
    finally:
        request_id_ctx.reset(token)
