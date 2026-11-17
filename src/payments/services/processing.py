import logging
from dataclasses import dataclass
from uuid import UUID

from payments.core.time import utcnow
from payments.db.session import session_scope
from payments.domain.enums import PaymentStatus
from payments.repositories.payment import PaymentRepository
from payments.schemas.events import WebhookPayload
from payments.schemas.payment import PaymentRead
from payments.services.gateway import PaymentGateway
from payments.services.webhook import WebhookNotifier

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _Settled:
    payment: PaymentRead
    webhook_delivered: bool


class PaymentProcessor:
    """Обработка события payments.new: шлюз → статус в БД → webhook.

    Каждый шаг идемпотентен, поэтому повторная доставка того же сообщения безопасна:
    списание уже обработанного платежа не повторяется, доставленный webhook не шлётся снова.
    """

    def __init__(self, gateway: PaymentGateway, webhooks: WebhookNotifier) -> None:
        self._gateway = gateway
        self._webhooks = webhooks

    async def process(self, payment_id: UUID) -> None:
        """Довести платёж до конечного статуса и уведомить клиента.

        :param payment_id: платёж из события
        :raises WebhookDeliveryError: клиент не принял уведомление — сообщение надо повторить
        """
        settled = await self._settle(payment_id)
        if settled is None:
            logger.warning(f'Payment {payment_id} not found, dropping event')
            return
        if settled.webhook_delivered:
            logger.info(f'Payment {payment_id} already notified, nothing to do')
            return
        await self._webhooks.send(
            settled.payment.webhook_url,
            WebhookPayload.for_payment(settled.payment),
        )
        async with session_scope() as session:
            await PaymentRepository(session).mark_webhook_delivered(payment_id, utcnow())
        logger.info(f'Payment {payment_id} webhook delivered')

    async def _settle(self, payment_id: UUID) -> _Settled | None:
        # Блокировка строки на время вызова шлюза: дубль сообщения в соседнем consumer
        # дождётся коммита и увидит конечный статус, второго списания не будет.
        async with session_scope() as session:
            payment = await PaymentRepository(session).lock(payment_id)
            if payment is None:
                return None
            if payment.status == PaymentStatus.PENDING:
                payment.status = await self._gateway.charge(payment)
                payment.processed_at = utcnow()
                await session.flush()
                logger.info(f'Payment {payment_id} processed: {payment.status}')
            return _Settled(
                payment=PaymentRead.from_model(payment),
                webhook_delivered=payment.webhook_delivered_at is not None,
            )
