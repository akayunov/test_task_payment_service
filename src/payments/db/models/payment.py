import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import CheckConstraint, DateTime, Index, Numeric, String, Text, Uuid, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from payments.db.base import Base, TimestampMixin
from payments.db.types import pg_enum
from payments.domain.enums import Currency, PaymentStatus


class Payment(TimestampMixin, Base):
    """Платёж. Статус меняет только consumer, один раз: pending → succeeded | failed."""

    __tablename__ = 'payments'
    __table_args__ = (
        CheckConstraint('amount > 0', name='amount_positive'),
        CheckConstraint(
            "(status = 'pending' AND processed_at IS NULL) "
            "OR (status <> 'pending' AND processed_at IS NOT NULL)",
            name='processed_at_matches_status',
        ),
        CheckConstraint(
            "webhook_delivered_at IS NULL OR status <> 'pending'",
            name='webhook_after_processing',
        ),
        Index('ix_payments_created_at', 'created_at'),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    currency: Mapped[Currency] = mapped_column(
        pg_enum(Currency, 'payment_currency'),
        nullable=False,
    )
    description: Mapped[str | None] = mapped_column(Text)
    # `metadata` занято у декларативных моделей SQLAlchemy, поэтому атрибут с подчёркиванием.
    metadata_: Mapped[dict[str, Any]] = mapped_column(
        'metadata',
        JSONB,
        default=dict,
        server_default=text("'{}'::jsonb"),
        nullable=False,
    )
    status: Mapped[PaymentStatus] = mapped_column(
        pg_enum(PaymentStatus, 'payment_status'),
        default=PaymentStatus.PENDING,
        server_default=text("'pending'::payment_status"),
        nullable=False,
    )
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    webhook_url: Mapped[str] = mapped_column(String(2048), nullable=False)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    webhook_delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
