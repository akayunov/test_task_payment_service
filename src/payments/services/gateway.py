import asyncio
import random
from typing import Protocol

from payments.core.config import Settings
from payments.db.models import Payment
from payments.domain.enums import PaymentStatus


class PaymentGateway(Protocol):
    async def charge(self, payment: Payment) -> PaymentStatus: ...


class EmulatedGateway:
    """Эмуляция внешнего шлюза: случайная задержка и заданная доля успешных списаний.

    Отказ шлюза — бизнес-результат (платёж failed), а не ошибка обработки: его не повторяем.
    """

    def __init__(
        self,
        min_delay: float,
        max_delay: float,
        success_rate: float,
        rng: random.Random | None = None,
    ) -> None:
        self._min_delay = min_delay
        self._max_delay = max_delay
        self._success_rate = success_rate
        self._rng = rng or random.Random()

    @classmethod
    def from_settings(cls, settings: Settings) -> 'EmulatedGateway':
        return cls(
            min_delay=settings.gateway_min_delay,
            max_delay=settings.gateway_max_delay,
            success_rate=settings.gateway_success_rate,
        )

    async def charge(self, payment: Payment) -> PaymentStatus:
        del payment  # настоящий шлюз получил бы сумму, валюту и id как ключ идемпотентности
        await asyncio.sleep(self._rng.uniform(self._min_delay, self._max_delay))
        if self._rng.random() < self._success_rate:
            return PaymentStatus.SUCCEEDED
        return PaymentStatus.FAILED
