from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from payments.domain.enums import PaymentStatus
from payments.schemas.payment import PaymentRead


class PaymentCreatedEvent(BaseModel):
    """Тело сообщения payments.new. Состояние платежа consumer всегда читает из БД."""

    # Новые поля от более свежего producer не должны ронять старый consumer.
    model_config = ConfigDict(extra='ignore')

    event_id: UUID
    payment_id: UUID
    occurred_at: datetime


class WebhookPayload(BaseModel):
    """Тело webhook. Повторы возможны, клиент дедуплицирует по payment.id."""

    event: Literal['payment.succeeded', 'payment.failed']
    payment: PaymentRead

    @classmethod
    def for_payment(cls, payment: PaymentRead) -> 'WebhookPayload':
        if payment.status == PaymentStatus.SUCCEEDED:
            return cls(event='payment.succeeded', payment=payment)
        if payment.status == PaymentStatus.FAILED:
            return cls(event='payment.failed', payment=payment)
        raise ValueError('Webhook is sent only for processed payments')
