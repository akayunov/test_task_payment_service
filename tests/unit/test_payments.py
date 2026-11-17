"""API платежей: приём, outbox в той же транзакции, идемпотентность, авторизация."""

import asyncio
from uuid import uuid4

import pytest
from fastapi import status

from payments.domain.enums import EventType
from tests.unit.helpers import (
    API_PREFIX,
    AUTH,
    WEBHOOK_URL,
    create_payment,
    outbox_rows,
    payment_body,
    post_payment,
)


async def test_create_returns_accepted_and_writes_outbox(client):
    response = await post_payment(client)

    assert response.status_code == status.HTTP_202_ACCEPTED, response.text
    body = response.json()
    assert set(body) == {'payment_id', 'status', 'created_at'}
    assert body['status'] == 'pending'
    assert response.headers['location'] == f'{API_PREFIX}/payments/{body["payment_id"]}'
    assert 'idempotent-replayed' not in response.headers

    rows = await outbox_rows()
    assert len(rows) == 1
    assert rows[0].event_type == EventType.PAYMENT_CREATED
    assert str(rows[0].aggregate_id) == body['payment_id']
    assert rows[0].payload['payment_id'] == body['payment_id']
    assert rows[0].payload['event_id'] == str(rows[0].id)
    assert rows[0].published_at is None


async def test_get_returns_details(client):
    created = await create_payment(client, amount=99.5, currency='USD', description='  ')

    response = await client.get(f'{API_PREFIX}/payments/{created["payment_id"]}', headers=AUTH)

    assert response.status_code == status.HTTP_200_OK
    body = response.json()
    assert body['id'] == created['payment_id']
    assert body['amount'] == '99.50'
    assert body['currency'] == 'USD'
    assert body['description'] is None
    assert body['metadata'] == {'order_id': 42}
    assert body['status'] == 'pending'
    assert body['webhook_url'] == WEBHOOK_URL
    assert body['processed_at'] is None
    assert body['idempotency_key']


async def test_get_unknown_payment_is_404(client):
    response = await client.get(f'{API_PREFIX}/payments/{uuid4()}', headers=AUTH)

    assert response.status_code == status.HTTP_404_NOT_FOUND
    assert response.json()['code'] == 'payment_not_found'


async def test_same_key_and_body_returns_same_payment(client):
    key = str(uuid4())
    first = await post_payment(client, idempotency_key=key)
    # 1500 и "1500.00" — одна и та же сумма.
    second = await post_payment(client, payment_body(amount=1500), idempotency_key=key)

    assert second.status_code == status.HTTP_202_ACCEPTED
    assert second.json() == first.json()
    assert second.headers['idempotent-replayed'] == 'true'
    assert len(await outbox_rows()) == 1


async def test_same_key_with_other_body_is_conflict(client):
    key = str(uuid4())
    await post_payment(client, idempotency_key=key)

    response = await post_payment(client, payment_body(amount='1.00'), idempotency_key=key)

    assert response.status_code == status.HTTP_409_CONFLICT
    assert response.json()['code'] == 'idempotency_key_reused'
    assert len(await outbox_rows()) == 1


async def test_concurrent_requests_with_same_key_create_one_payment(client):
    key = str(uuid4())

    responses = await asyncio.gather(
        *(post_payment(client, idempotency_key=key) for _ in range(5)),
    )

    assert {response.status_code for response in responses} == {status.HTTP_202_ACCEPTED}
    assert len({response.json()['payment_id'] for response in responses}) == 1
    assert len(await outbox_rows()) == 1


@pytest.mark.parametrize(
    'headers',
    [
        {'Idempotency-Key': 'k-1'},
        {'Idempotency-Key': 'k-1', 'X-API-Key': 'wrong-key'},
    ],
    ids=['missing', 'wrong'],
)
async def test_api_key_is_required(client, headers):
    created = await post_payment(client, headers=headers)
    fetched = await client.get(
        f'{API_PREFIX}/payments/{uuid4()}',
        headers={k: v for k, v in headers.items() if k == 'X-API-Key'},
    )

    assert created.status_code == status.HTTP_401_UNAUTHORIZED
    assert fetched.status_code == status.HTTP_401_UNAUTHORIZED
    assert created.json()['code'] in {'api_key_missing', 'api_key_invalid'}
    assert await outbox_rows() == []


async def test_idempotency_key_is_required(client):
    response = await post_payment(client, headers=AUTH)

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    assert response.json()['errors'][0]['loc'] == ['header', 'Idempotency-Key']


@pytest.mark.parametrize(
    'overrides',
    [
        {'amount': 0},
        {'amount': '-1'},
        {'amount': '10.001'},
        {'amount': 'NaN'},
        {'currency': 'GBP'},
        {'webhook_url': 'not-a-url'},
        {'webhook_url': 'ftp://example.com/hook'},
        {'description': 'x' * 1001},
        {'metadata': {'blob': 'x' * 17_000}},
        {'unexpected': True},
    ],
    ids=[
        'zero',
        'negative',
        'three-decimals',
        'nan',
        'currency',
        'url',
        'url-scheme',
        'description',
        'metadata-size',
        'extra-field',
    ],
)
async def test_invalid_body_is_rejected(client, overrides):
    response = await post_payment(client, payment_body(**overrides))

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT, response.text
    assert response.json()['code'] == 'validation_error'
    assert await outbox_rows() == []
