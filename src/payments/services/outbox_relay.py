import asyncio
import json
import logging

from payments.core.time import utcnow
from payments.db.session import session_scope
from payments.messaging.broker import MessagePublisher
from payments.messaging.retry import describe_error
from payments.messaging.topology import PAYMENTS_EXCHANGE
from payments.repositories.outbox import OutboxRepository

logger = logging.getLogger(__name__)


class OutboxRelay:
    """Переносит события из таблицы outbox в RabbitMQ.

    Строка помечается опубликованной только после подтверждения брокера и в той же
    транзакции, что держит блокировку. Падение между подтверждением и коммитом даст
    повторную публикацию — доставка at-least-once, consumer к дублям готов.
    """

    def __init__(self, publisher: MessagePublisher, batch_size: int) -> None:
        self._publisher = publisher
        self._batch_size = batch_size

    async def publish_pending(self) -> int:
        """Опубликовать одну пачку.

        На первой ошибке пачка останавливается: брокер, скорее всего, недоступен,
        и остальные строки лучше отдать следующей итерации.

        :return: сколько событий опубликовано
        """
        published = 0
        async with session_scope() as session:
            messages = await OutboxRepository(session).lock_unpublished(self._batch_size)
            for message in messages:
                try:
                    await self._publisher.publish(
                        json.dumps(message.payload).encode(),
                        exchange=PAYMENTS_EXCHANGE,
                        routing_key=message.event_type.value,
                        message_id=str(message.id),
                    )
                except Exception as exc:
                    message.attempts += 1
                    message.last_error = describe_error(exc)
                    logger.warning(f'Outbox message {message.id} publish failed: {exc!r}')
                    break
                message.published_at = utcnow()
                published += 1
        if published:
            logger.info(f'Published {published} outbox message(s)')
        return published

    async def run(self, poll_interval: float, stop: asyncio.Event) -> None:
        """Опрашивать outbox, пока не выставлен stop. Полная пачка — сразу следующая."""
        while not stop.is_set():
            try:
                published = await self.publish_pending()
            except Exception:
                logger.exception('Outbox relay iteration failed')
                published = 0
            if published >= self._batch_size:
                continue
            try:
                await asyncio.wait_for(stop.wait(), timeout=poll_interval)
            except TimeoutError:
                pass
