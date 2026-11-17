import logging
from typing import Any

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from payments.core.exceptions import AppError
from payments.core.logging import request_id_ctx

logger = logging.getLogger(__name__)

_HTTP_CODES = {
    status.HTTP_401_UNAUTHORIZED: 'unauthorized',
    status.HTTP_404_NOT_FOUND: 'not_found',
    status.HTTP_405_METHOD_NOT_ALLOWED: 'method_not_allowed',
}


def register_exception_handlers(app: FastAPI) -> None:
    """Подключить единый формат ошибок."""
    app.add_exception_handler(AppError, _handle_app_error)
    app.add_exception_handler(HTTPException, _handle_http_exception)
    app.add_exception_handler(StarletteHTTPException, _handle_http_exception)
    app.add_exception_handler(RequestValidationError, _handle_validation_error)
    app.add_exception_handler(Exception, _handle_unexpected_error)


async def _handle_app_error(request: Request, exc: Exception) -> JSONResponse:
    if not isinstance(exc, AppError):  # подсказка для тайп линтера
        raise exc
    return JSONResponse(
        status_code=exc.status_code,
        content=_body(request, detail=exc.message, code=exc.code),
    )


async def _handle_http_exception(request: Request, exc: Exception) -> JSONResponse:
    if not isinstance(exc, StarletteHTTPException):  # подсказка для тайп линтера
        raise exc
    detail = exc.detail if isinstance(exc.detail, str) else 'Request failed'
    code = _HTTP_CODES.get(exc.status_code, 'http_error')
    return JSONResponse(
        status_code=exc.status_code,
        content=_body(request, detail=detail, code=code),
    )


async def _handle_validation_error(request: Request, exc: Exception) -> JSONResponse:
    if not isinstance(exc, RequestValidationError):  # подсказка для тайп линтера
        raise exc
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        content=_body(
            request,
            detail='Request validation failed',
            code='validation_error',
            errors=jsonable_encoder(exc.errors()),
        ),
    )


async def _handle_unexpected_error(request: Request, exc: Exception) -> JSONResponse:
    logger.exception(f'Unhandled error on {request.method} {request.url.path}')
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content=_body(request, detail='Internal server error', code='internal_error'),
    )


def _body(request: Request, **payload: Any) -> dict[str, Any]:
    del request
    request_id = request_id_ctx.get()
    if request_id != '-':
        payload['request_id'] = request_id
    return payload
