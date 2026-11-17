import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]

os.environ['ENVIRONMENT'] = 'test'
os.environ['LOG_LEVEL'] = 'WARNING'
os.environ.setdefault('API_KEY', 'test-api-key-at-least-16-chars')
os.environ.setdefault(
    'DATABASE_URL',
    'postgresql+asyncpg://payments:payments@localhost:5432/payments',
)


def pytest_sessionstart(session: pytest.Session) -> None:
    """Накатить миграции на базу из DATABASE_URL."""
    del session  # обязательный аргумент хука pytest
    result = subprocess.run(
        [sys.executable, '-m', 'alembic', 'upgrade', 'head'],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        details = f'{result.stdout}\n{result.stderr}'
        raise RuntimeError(details)


@pytest.fixture(autouse=True)
async def clean_database():
    """Очистить таблицы и закрыть пул: следующий тест получит свой event loop."""
    yield
    from sqlalchemy import text

    from payments.db.session import dispose_engine, get_engine

    engine = get_engine()
    async with engine.begin() as connection:
        await connection.execute(text('TRUNCATE TABLE payments, outbox'))
    await dispose_engine()


@pytest.fixture
async def client():
    from httpx import ASGITransport, AsyncClient

    from payments.main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url='http://test') as async_client:
        yield async_client
