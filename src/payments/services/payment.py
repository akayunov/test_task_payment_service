from dataclasses import dataclass
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from payments.core.exceptions import ConflictError, NotFoundError
from payments.core.time import utcnow
from payments.db.models import OutboxMessage, Payment
from payments.domain.enums import EventType, PaymentStatus
from payments.repositories.outbox import OutboxRepository
from payments.repositories.payment import PaymentRepository
from payments.schemas.events import PaymentCreatedEvent
from payments.schemas.payment import PaymentAccepted, PaymentCreate, PaymentRead


@dataclass(frozen=True)
class CreatePaymentResult:
    payment: PaymentAccepted
    replayed: bool


class PaymentService:
    """Приём платежей и чтение их состояния."""

    def __init__(self, session: AsyncSession) -> None:
        self._payments = PaymentRepository(session)
        self._outbox = OutboxRepository(session)

    async def create(self, idempotency_key: str, data: PaymentCreate) -> CreatePaymentResult:
        """Принять платёж и записать событие payments.new в outbox той же транзакцией.

        Повтор с тем же ключом и тем же телом возвращает уже созданный платёж
        и не создаёт второго события.

        :param idempotency_key: значение заголовка Idempotency-Key
        :param data: тело запроса
        :raises ConflictError: ключ уже использован с другим телом
        :return: принятый платёж и признак повтора
        """
        payment_id = uuid4()
        inserted = await self._payments.insert_if_absent(
            id=payment_id,
            amount=data.amount,
            currency=data.currency,
            description=data.description,
            metadata_=data.metadata,
            status=PaymentStatus.PENDING,
            idempotency_key=idempotency_key,
            webhook_url=str(data.webhook_url),
        )
        if inserted is not None:
            await self._enqueue_created(payment_id)
            payment = await self._require(payment_id)
            return CreatePaymentResult(PaymentAccepted.from_model(payment), replayed=False)

        existing = await self._payments.get_by_idempotency_key(idempotency_key)
        if existing is None:
            # Строку с ключом удалили между INSERT и SELECT — удаления в сервисе нет.
            raise ConflictError('Concurrent request, retry later', code='idempotency_race')
        if not data.matches(existing):
            raise ConflictError(
                'Idempotency key was already used with a different request body',
                code='idempotency_key_reused',
            )
        return CreatePaymentResult(PaymentAccepted.from_model(existing), replayed=True)

    async def get(self, payment_id: UUID) -> PaymentRead:
        """Состояние платежа.

        :param payment_id: первичный ключ
        :raises NotFoundError: платёж не найден
        :return: платёж
        """
        return PaymentRead.from_model(await self._require(payment_id))

    async def _enqueue_created(self, payment_id: UUID) -> None:
        event_id = uuid4()
        event = PaymentCreatedEvent(event_id=event_id, payment_id=payment_id, occurred_at=utcnow())
        await self._outbox.add(
            OutboxMessage(
                id=event_id,
                event_type=EventType.PAYMENT_CREATED,
                aggregate_id=payment_id,
                payload=event.model_dump(mode='json'),
            )
        )

    async def _require(self, payment_id: UUID) -> Payment:
        payment = await self._payments.get(payment_id)
        if payment is None:
            raise NotFoundError('Payment not found', code='payment_not_found')
        return payment
