from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from faststream.rabbit import RabbitQueue

from payments.core.config import Settings
from payments.messaging.topology import retry_queue

ATTEMPT_HEADER = 'x-attempt'
ERROR_HEADER = 'x-last-error'
ERROR_HEADER_MAX_LENGTH = 1000


@dataclass(frozen=True)
class RetryPolicy:
    """Экспоненциальная задержка: base, 2·base, 4·base… до max_attempts попыток всего."""

    max_attempts: int
    base_delay: float

    @classmethod
    def from_settings(cls, settings: Settings) -> 'RetryPolicy':
        return cls(max_attempts=settings.retry_max_attempts, base_delay=settings.retry_base_delay)

    def should_retry(self, attempt: int) -> bool:
        """Есть ли ещё попытка после неудачной попытки attempt (нумерация с 1)."""
        return attempt < self.max_attempts

    def delay_ms(self, attempt: int) -> int:
        """Задержка перед попыткой attempt + 1."""
        return round(self.base_delay * 1000 * 2 ** (attempt - 1))

    def retry_queues(self) -> list[RabbitQueue]:
        return [retry_queue(self.delay_ms(attempt)) for attempt in range(1, self.max_attempts)]


def attempt_from_headers(headers: Mapping[str, Any]) -> int:
    """Номер текущей попытки. Сообщение от relay заголовка не несёт — это первая попытка."""
    try:
        attempt = int(headers.get(ATTEMPT_HEADER, 1))
    except (TypeError, ValueError):
        return 1
    return max(attempt, 1)


def describe_error(exc: BaseException) -> str:
    return f'{type(exc).__name__}: {exc}'[:ERROR_HEADER_MAX_LENGTH]
