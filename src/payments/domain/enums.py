import enum


class Currency(enum.StrEnum):
    RUB = 'RUB'
    USD = 'USD'
    EUR = 'EUR'


class PaymentStatus(enum.StrEnum):
    PENDING = 'pending'
    SUCCEEDED = 'succeeded'
    FAILED = 'failed'


class EventType(enum.StrEnum):
    """Тип события в outbox. Значение служит routing key в RabbitMQ."""

    PAYMENT_CREATED = 'payments.new'
