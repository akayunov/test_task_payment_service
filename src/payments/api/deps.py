from typing import Annotated

from fastapi import Depends, Security
from fastapi.security import APIKeyHeader
from sqlalchemy.ext.asyncio import AsyncSession

from payments.core.security import verify_api_key
from payments.db.session import get_db

api_key_header = APIKeyHeader(name='X-API-Key', auto_error=False)

# scope='function': коммит до отправки ответа. Иначе клиент получит 202 раньше коммита,
# сразу следующий GET не найдёт платёж, а ошибка коммита до клиента не дойдёт.
DbSession = Annotated[AsyncSession, Depends(get_db, scope='function')]


async def require_api_key(api_key: Annotated[str | None, Security(api_key_header)]) -> None:
    """Пропустить запрос только со статическим ключом из настроек.

    :param api_key: значение X-API-Key или None
    :raises UnauthorizedError: ключ отсутствует или неверен
    """
    verify_api_key(api_key)
