"""Обменники и очереди RabbitMQ.

payments (direct) --payments.new--> payments.new --(reject)--> payments.dlx --> payments.new.dlq
payments.retry (direct) --<имя очереди>--> payments.new.retry.<N>ms --(TTL)--> payments
"""

from collections.abc import Iterable

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from payments.domain.enums import EventType

PAYMENT_CREATED_ROUTING_KEY = EventType.PAYMENT_CREATED.value

PAYMENTS_EXCHANGE = RabbitExchange('payments', type=ExchangeType.DIRECT, durable=True)
RETRY_EXCHANGE = RabbitExchange('payments.retry', type=ExchangeType.DIRECT, durable=True)
DEAD_LETTER_EXCHANGE = RabbitExchange('payments.dlx', type=ExchangeType.DIRECT, durable=True)

# Сообщение, которое consumer не смог даже разобрать, брокер сам отправит в DLQ.
PAYMENTS_QUEUE = RabbitQueue(
    PAYMENT_CREATED_ROUTING_KEY,
    durable=True,
    routing_key=PAYMENT_CREATED_ROUTING_KEY,
    arguments={
        'x-dead-letter-exchange': DEAD_LETTER_EXCHANGE.name,
        'x-dead-letter-routing-key': PAYMENT_CREATED_ROUTING_KEY,
    },
)
DEAD_LETTER_QUEUE = RabbitQueue(
    f'{PAYMENT_CREATED_ROUTING_KEY}.dlq',
    durable=True,
    routing_key=PAYMENT_CREATED_ROUTING_KEY,
)


def retry_queue(delay_ms: int) -> RabbitQueue:
    """Очередь отложенной повторной доставки без потребителей.

    TTL зашит в аргументы очереди, а аргументы существующей очереди менять нельзя,
    поэтому задержка входит в имя: новая настройка создаёт новую очередь, а не ошибку.
    Отдельная очередь на каждую задержку убирает блокировку головы очереди,
    которая бывает при TTL на отдельных сообщениях.

    :param delay_ms: задержка перед возвратом в основную очередь
    :return: описание очереди
    """
    name = f'{PAYMENT_CREATED_ROUTING_KEY}.retry.{delay_ms}ms'
    return RabbitQueue(
        name,
        durable=True,
        routing_key=name,
        arguments={
            'x-message-ttl': delay_ms,
            'x-dead-letter-exchange': PAYMENTS_EXCHANGE.name,
            'x-dead-letter-routing-key': PAYMENT_CREATED_ROUTING_KEY,
        },
    )


async def declare_topology(broker: RabbitBroker, retry_queues: Iterable[RabbitQueue]) -> None:
    """Объявить обменники, очереди и привязки. Повторный вызов ничего не меняет.

    Объявляют и relay, и consumer: кто бы ни стартовал первым, публикация в payments
    не потеряется из-за отсутствующей очереди.

    :param broker: подключённый брокер
    :param retry_queues: очереди задержки из политики повторов
    """
    payments = await broker.declare_exchange(PAYMENTS_EXCHANGE)
    retry = await broker.declare_exchange(RETRY_EXCHANGE)
    dead_letter = await broker.declare_exchange(DEAD_LETTER_EXCHANGE)

    main_queue = await broker.declare_queue(PAYMENTS_QUEUE)
    await main_queue.bind(payments, routing_key=PAYMENT_CREATED_ROUTING_KEY)

    dlq = await broker.declare_queue(DEAD_LETTER_QUEUE)
    await dlq.bind(dead_letter, routing_key=PAYMENT_CREATED_ROUTING_KEY)

    for queue in retry_queues:
        declared = await broker.declare_queue(queue)
        await declared.bind(retry, routing_key=queue.name)
