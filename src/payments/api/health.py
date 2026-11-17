from fastapi import APIRouter, status
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from payments.api.deps import DbSession

router = APIRouter(tags=['health'])


@router.get('/health')
async def health() -> dict[str, str]:
    """Процесс жив и принимает запросы."""
    return {'status': 'ok'}


@router.get('/ready', response_model=None)
async def readiness(session: DbSession) -> JSONResponse:
    """База отвечает. RabbitMQ API не нужен: события уходят через outbox."""
    try:
        await session.execute(text('SELECT 1'))
    except SQLAlchemyError:
        await session.rollback()
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={'status': 'not_ready', 'code': 'database_unavailable'},
        )
    return JSONResponse(content={'status': 'ready'})
