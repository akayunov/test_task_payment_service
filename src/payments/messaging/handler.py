import logging

from payments.messaging.broker import MessagePublisher
from payments.messaging.retry import ATTEMPT_HEADER, ERROR_HEADER, RetryPolicy, describe_error
from payments.messaging.topology import (
    DEAD_LETTER_EXCHANGE,
    PAYMENT_CREATED_ROUTING_KEY,
    RETRY_EXCHANGE,
    retry_queue,
)
from payments.schemas.events import PaymentCreatedEvent
from payments.services.processing import PaymentProcessor

logger = logging.getLogger(__name__)


class PaymentEventHandler:
    """Единственный обработчик payments.new: обработка, а при ошибке — retry или DLQ.

    Повтор — это новая публикация исходного тела в очередь задержки с x-attempt + 1.
    Исходное сообщение после этого подтверждается. Если публикация повтора сама упала,
    исключение уходит в FastStream, и тот отклоняет сообщение в DLQ.
    """

    def __init__(
        self,
        processor: PaymentProcessor,
        publisher: MessagePublisher,
        policy: RetryPolicy,
    ) -> None:
        self._processor = processor
        self._publisher = publisher
        self._policy = policy

    async def handle(
        self,
        event: PaymentCreatedEvent,
        *,
        body: bytes,
        attempt: int,
        message_id: str,
    ) -> None:
        """Обработать событие.

        :param event: разобранное тело
        :param body: исходное тело, уходит в retry/DLQ без изменений
        :param attempt: номер попытки, с 1
        :param message_id: id сообщения, сохраняется во всех повторах
        """
        try:
            await self._processor.process(event.payment_id)
        except Exception as exc:
            await self._on_failure(
                event,
                body=body,
                attempt=attempt,
                message_id=message_id,
                exc=exc,
            )

    async def _on_failure(
        self,
        event: PaymentCreatedEvent,
        *,
        body: bytes,
        attempt: int,
        message_id: str,
        exc: Exception,
    ) -> None:
        error = describe_error(exc)
        if self._policy.should_retry(attempt):
            queue = retry_queue(self._policy.delay_ms(attempt))
            logger.warning(
                f'Payment {event.payment_id} attempt {attempt}/{self._policy.max_attempts} '
                f'failed ({error}), retry via {queue.name}'
            )
            await self._publisher.publish(
                body,
                exchange=RETRY_EXCHANGE,
                routing_key=queue.name,
                message_id=message_id,
                headers={ATTEMPT_HEADER: attempt + 1, ERROR_HEADER: error},
            )
            return
        logger.error(
            f'Payment {event.payment_id} failed after {attempt} attempt(s) ({error}), '
            'moving to dead letter queue'
        )
        await self._publisher.publish(
            body,
            exchange=DEAD_LETTER_EXCHANGE,
            routing_key=PAYMENT_CREATED_ROUTING_KEY,
            message_id=message_id,
            headers={ATTEMPT_HEADER: attempt, ERROR_HEADER: error},
        )
