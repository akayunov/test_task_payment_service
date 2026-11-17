from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from payments.db.models import OutboxMessage


class OutboxRepository:
    """Запросы к таблице outbox."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, message: OutboxMessage) -> OutboxMessage:
        """Записать событие в текущую транзакцию.

        :param message: новая строка outbox
        :return: строка после flush
        """
        self._session.add(message)
        await self._session.flush()
        return message

    async def lock_unpublished(self, limit: int) -> list[OutboxMessage]:
        """Взять пачку неопубликованных событий, пропуская строки других relay.

        :param limit: размер пачки
        :return: строки в порядке создания, заблокированные до конца транзакции
        """
        statement = (
            select(OutboxMessage)
            .where(OutboxMessage.published_at.is_(None))
            .order_by(OutboxMessage.created_at, OutboxMessage.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        result = await self._session.execute(statement)
        return list(result.scalars().all())
