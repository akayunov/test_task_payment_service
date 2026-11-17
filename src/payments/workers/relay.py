"""Outbox relay. Запуск: faststream run payments.workers.relay:app"""

import asyncio

from faststream import FastStream

from payments.core.config import get_settings
from payments.core.logging import configure_logging
from payments.db.session import dispose_engine
from payments.messaging.broker import BrokerPublisher, build_broker
from payments.messaging.retry import RetryPolicy
from payments.messaging.topology import declare_topology
from payments.services.outbox_relay import OutboxRelay

configure_logging()
settings = get_settings()
broker = build_broker(settings)
app = FastStream(broker)

_stop = asyncio.Event()
_tasks: set[asyncio.Task[None]] = set()


@app.on_startup
async def setup() -> None:
    await broker.connect()
    await declare_topology(broker, RetryPolicy.from_settings(settings).retry_queues())


@app.after_startup
async def start_relay() -> None:
    relay = OutboxRelay(BrokerPublisher(broker), batch_size=settings.outbox_batch_size)
    task = asyncio.create_task(relay.run(settings.outbox_poll_interval, _stop))
    _tasks.add(task)


@app.on_shutdown
async def stop_relay() -> None:
    # Пачка дописывается до конца: публиковать после закрытия брокера нельзя.
    _stop.set()
    await asyncio.gather(*_tasks)
    _tasks.clear()


@app.after_shutdown
async def teardown() -> None:
    await dispose_engine()
