import logging
from typing import Any, Protocol

from faststream.rabbit import Channel, RabbitBroker, RabbitExchange

from payments.core.config import Settings


def build_broker(settings: Settings) -> RabbitBroker:
    """Брокер с подтверждениями публикации.

    publisher confirms + mandatory + on_return_raises: publish() возвращается только
    после того, как RabbitMQ положил сообщение в очередь, а не просто принял по сети.
    """
    return RabbitBroker(
        settings.rabbitmq_url,
        default_channel=Channel(
            prefetch_count=settings.rabbitmq_prefetch_count,
            publisher_confirms=True,
            on_return_raises=True,
        ),
        # Даём обработчику доработать сообщение при остановке: шлюз до 5 с плюс webhook.
        graceful_timeout=settings.gateway_max_delay + settings.webhook_timeout + 5,
        logger=logging.getLogger('payments.broker'),
    )


class MessagePublisher(Protocol):
    async def publish(
        self,
        body: bytes,
        *,
        exchange: RabbitExchange,
        routing_key: str,
        message_id: str,
        headers: dict[str, Any] | None = None,
    ) -> None: ...


class BrokerPublisher:
    """Публикация готового JSON-тела в RabbitMQ с сохранением на диск."""

    def __init__(self, broker: RabbitBroker) -> None:
        self._broker = broker

    async def publish(
        self,
        body: bytes,
        *,
        exchange: RabbitExchange,
        routing_key: str,
        message_id: str,
        headers: dict[str, Any] | None = None,
    ) -> None:
        await self._broker.publish(
            body,
            exchange=exchange,
            routing_key=routing_key,
            message_id=message_id,
            headers=headers,
            content_type='application/json',
            persist=True,
            mandatory=True,
        )
