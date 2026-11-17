import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, Index, Integer, Text, Uuid, func, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from payments.core.time import utcnow
from payments.db.base import Base
from payments.db.types import pg_enum
from payments.domain.enums import EventType


class OutboxMessage(Base):
    """Событие, записанное в одной транзакции с бизнес-данными. Публикует outbox relay."""

    __tablename__ = 'outbox'
    __table_args__ = (
        # Relay читает только неопубликованные строки — частичный индекс остаётся маленьким.
        Index(
            'ix_outbox_unpublished',
            'created_at',
            postgresql_where=text('published_at IS NULL'),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    event_type: Mapped[EventType] = mapped_column(
        pg_enum(EventType, 'outbox_event_type'),
        nullable=False,
    )
    aggregate_id: Mapped[uuid.UUID] = mapped_column(Uuid, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utcnow,
        server_default=func.now(),
        nullable=False,
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempts: Mapped[int] = mapped_column(
        Integer,
        default=0,
        server_default=text('0'),
        nullable=False,
    )
    last_error: Mapped[str | None] = mapped_column(Text)
