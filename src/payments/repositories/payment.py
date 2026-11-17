from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from payments.db.models import Payment


class PaymentRepository:
    """Запросы платежей."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert_if_absent(self, **values: Any) -> UUID | None:
        """Вставить платёж, если idempotency key ещё не занят.

        Параллельный запрос с тем же ключом ждёт на уникальном индексе,
        пока первая транзакция не закончится, и получает конфликт без ошибки.

        :param values: колонки нового платежа
        :return: id новой строки или None, если ключ уже занят
        """
        statement = (
            insert(Payment)
            .values(**values)
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment.id)
        )
        result = await self._session.execute(statement)
        return result.scalar_one_or_none()

    async def get(self, payment_id: UUID) -> Payment | None:
        """Загрузить платёж.

        :param payment_id: первичный ключ
        :return: платёж или None
        """
        statement = (
            select(Payment)
            .where(Payment.id == payment_id)
            .execution_options(populate_existing=True)
        )
        result = await self._session.execute(statement)
        return result.scalar_one_or_none()

    async def get_by_idempotency_key(self, idempotency_key: str) -> Payment | None:
        """Найти платёж по ключу идемпотентности.

        :param idempotency_key: значение заголовка Idempotency-Key
        :return: платёж или None
        """
        statement = select(Payment).where(Payment.idempotency_key == idempotency_key)
        result = await self._session.execute(statement)
        return result.scalar_one_or_none()

    async def lock(self, payment_id: UUID) -> Payment | None:
        """Заблокировать строку до конца транзакции.

        :param payment_id: первичный ключ
        :return: платёж или None
        """
        statement = (
            select(Payment)
            .where(Payment.id == payment_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        result = await self._session.execute(statement)
        return result.scalar_one_or_none()

    async def mark_webhook_delivered(self, payment_id: UUID, delivered_at: datetime) -> None:
        """Отметить доставку webhook. Первая отметка не перезаписывается.

        :param payment_id: первичный ключ
        :param delivered_at: время успешного ответа клиента
        """
        statement = (
            update(Payment)
            .where(Payment.id == payment_id, Payment.webhook_delivered_at.is_(None))
            .values(webhook_delivered_at=delivered_at)
        )
        await self._session.execute(statement)
