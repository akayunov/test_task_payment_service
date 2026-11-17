"""E2E: приём платежей, чтение, идемпотентность, авторизация."""

from http import HTTPStatus
from uuid import uuid4

import pytest

from tests.e2e.client import auth_headers
from tests.e2e.resources.base import ANY
from tests.e2e.resources.payment import Payment

pytestmark = pytest.mark.e2e


def test_create_and_get():
    payment = Payment(amount='99.90', currency='EUR', metadata={'order_id': 7}).create()

    payment.get(expected_body={'amount': '99.90', 'created_at': ANY})


def test_repeat_with_same_key_returns_same_payment():
    payment = Payment().create()
    replay = Payment(sink=payment.sink, metadata=payment.metadata)
    replay.idempotency_key = payment.idempotency_key

    replay.create(
        expected_headers={'idempotent-replayed': 'true'},
        expected_body={'payment_id': payment.id},
    )


def test_same_key_with_other_body_is_conflict():
    payment = Payment().create()

    payment.create(
        payload=payment.payload(amount='1.00'),
        status_code=HTTPStatus.CONFLICT,
        expected_body={'code': 'idempotency_key_reused'},
    )


@pytest.mark.parametrize(
    ('headers', 'code'),
    [({}, 'api_key_missing'), ({'X-API-Key': 'wrong-key'}, 'api_key_invalid')],
    ids=['missing', 'wrong'],
)
def test_api_key_is_required(headers, code):
    Payment().create(
        headers={**headers, 'Idempotency-Key': str(uuid4())},
        status_code=HTTPStatus.UNAUTHORIZED,
        expected_body={'code': code},
    )
    Payment(id=str(uuid4())).get(
        headers=headers,
        status_code=HTTPStatus.UNAUTHORIZED,
        expected_body={'code': code},
    )


def test_idempotency_key_is_required():
    Payment().create(
        headers=auth_headers(),
        status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
        expected_body={'code': 'validation_error'},
    )


@pytest.mark.parametrize(
    'overrides',
    [{'amount': '0'}, {'amount': '1.001'}, {'currency': 'GBP'}, {'webhook_url': 'nope'}],
    ids=['zero-amount', 'precision', 'currency', 'webhook-url'],
)
def test_invalid_body_is_rejected(overrides):
    payment = Payment()

    payment.create(
        payload=payment.payload(**overrides),
        status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
        expected_body={'code': 'validation_error'},
    )


def test_unknown_payment_is_404():
    Payment(id=str(uuid4())).get(
        status_code=HTTPStatus.NOT_FOUND,
        expected_body={'code': 'payment_not_found'},
    )
