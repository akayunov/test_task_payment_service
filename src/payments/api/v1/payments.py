from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Header, Response, status

from payments.api.deps import DbSession
from payments.core.config import get_settings
from payments.schemas.payment import PaymentAccepted, PaymentCreate, PaymentRead
from payments.services.payment import PaymentService

router = APIRouter()

IdempotencyKey = Annotated[
    str,
    Header(
        alias='Idempotency-Key',
        min_length=1,
        max_length=255,
        description='Повтор с тем же ключом и телом вернёт тот же платёж',
    ),
]


@router.post(
    '',
    status_code=status.HTTP_202_ACCEPTED,
    responses={status.HTTP_409_CONFLICT: {'description': 'Ключ уже использован с другим телом'}},
)
async def create_payment(
    payload: PaymentCreate,
    idempotency_key: IdempotencyKey,
    response: Response,
    session: DbSession,
) -> PaymentAccepted:
    """Принять платёж в обработку. Результат придёт на webhook_url."""
    result = await PaymentService(session).create(idempotency_key, payload)
    prefix = get_settings().api_prefix
    response.headers['Location'] = f'{prefix}/payments/{result.payment.payment_id}'
    if result.replayed:
        response.headers['Idempotent-Replayed'] = 'true'
    return result.payment


@router.get('/{payment_id}')
async def get_payment(payment_id: UUID, session: DbSession) -> PaymentRead:
    """Текущее состояние платежа."""
    return await PaymentService(session).get(payment_id)
