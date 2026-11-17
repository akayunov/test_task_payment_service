from sqlalchemy.orm import configure_mappers

from payments.db.models.outbox import OutboxMessage
from payments.db.models.payment import Payment

# Проверяем маппинг сразу, а не на первом запросе.
configure_mappers()

__all__ = ['OutboxMessage', 'Payment']
