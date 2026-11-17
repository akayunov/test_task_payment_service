from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

from payments.domain.enums import Currency, PaymentStatus

if TYPE_CHECKING:
    from payments.db.models import Payment

METADATA_MAX_BYTES = 16 * 1024


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra='forbid')

    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2, examples=['1500.00'])
    currency: Currency
    description: str | None = Field(default=None, max_length=1000, examples=['Order #42'])
    metadata: dict[str, Any] = Field(default_factory=dict, examples=[{'order_id': 42}])
    webhook_url: HttpUrl = Field(examples=['https://merchant.example.com/webhooks/payments'])

    @field_validator('description')
    @classmethod
    def blank_description_is_none(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = value.strip()
        return cleaned or None

    @field_validator('metadata')
    @classmethod
    def limit_metadata_size(cls, value: dict[str, Any]) -> dict[str, Any]:
        if len(json.dumps(value, ensure_ascii=False).encode()) > METADATA_MAX_BYTES:
            raise ValueError(f'metadata must not exceed {METADATA_MAX_BYTES} bytes as JSON')
        return value

    def matches(self, payment: Payment) -> bool:
        """Совпадает ли тело запроса с уже сохранённым платежом.

        :param payment: платёж с тем же idempotency key
        :return: True, если повтор запроса тот же самый
        """
        return (
            payment.amount == self.amount
            and payment.currency == self.currency
            and payment.description == self.description
            and payment.metadata_ == self.metadata
            and payment.webhook_url == str(self.webhook_url)
        )


class PaymentAccepted(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    created_at: datetime

    @classmethod
    def from_model(cls, payment: Payment) -> PaymentAccepted:
        return cls(payment_id=payment.id, status=payment.status, created_at=payment.created_at)


class PaymentRead(BaseModel):
    id: UUID
    amount: Decimal
    currency: Currency
    description: str | None
    metadata: dict[str, Any]
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None

    @classmethod
    def from_model(cls, payment: Payment) -> PaymentRead:
        return cls(
            id=payment.id,
            amount=payment.amount,
            currency=payment.currency,
            description=payment.description,
            metadata=payment.metadata_,
            status=payment.status,
            idempotency_key=payment.idempotency_key,
            webhook_url=payment.webhook_url,
            created_at=payment.created_at,
            processed_at=payment.processed_at,
        )
