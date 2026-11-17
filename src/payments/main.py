import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import RedirectResponse

from payments.api.errors import register_exception_handlers
from payments.api.health import router as health_router
from payments.api.middleware import RequestContextMiddleware
from payments.api.router import api_router
from payments.core.config import get_settings
from payments.core.logging import configure_logging
from payments.db.session import dispose_engine

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    """Закрыть пул соединений при остановке."""
    configure_logging()
    settings = get_settings()
    logger.info(f'Starting {settings.app_name} ({settings.environment})')
    yield
    logger.info('Stopping application')
    await dispose_engine()


async def redirect_to_docs() -> RedirectResponse:
    """Открыть Swagger с корня."""
    return RedirectResponse('/docs')


def create_app() -> FastAPI:
    """Собрать приложение: middleware, ошибки, роуты."""
    configure_logging()
    settings = get_settings()
    application = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        description=(
            'Asynchronous payment processing. A payment is accepted with 202, '
            'processed by a background consumer, and the result is sent to webhook_url.'
        ),
        lifespan=lifespan,
        openapi_tags=[
            {'name': 'health', 'description': 'Liveness and readiness probes'},
            {'name': 'payments', 'description': 'Payment intake and status'},
        ],
    )
    application.add_middleware(RequestContextMiddleware)
    register_exception_handlers(application)
    application.include_router(health_router)
    application.include_router(api_router, prefix=settings.api_prefix)
    application.add_api_route('/', redirect_to_docs, include_in_schema=False)
    return application


app = create_app()
