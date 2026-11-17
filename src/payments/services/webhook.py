from typing import Protocol

import httpx

from payments.core.config import Settings
from payments.schemas.events import WebhookPayload


class WebhookDeliveryError(Exception):
    """Клиент не принял уведомление: сеть, таймаут или ответ не 2xx."""


class WebhookNotifier(Protocol):
    async def send(self, url: str, payload: WebhookPayload) -> None: ...


def build_http_client(settings: Settings) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=settings.webhook_timeout,
        # Редирект мог бы увести запрос на внутренний адрес — не следуем.
        follow_redirects=False,
        headers={'User-Agent': f'payment-service/{settings.app_version}'},
    )


class WebhookSender:
    """POST JSON на webhook_url платежа. Успех — любой 2xx."""

    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    async def send(self, url: str, payload: WebhookPayload) -> None:
        """Отправить уведомление.

        :param url: адрес клиента
        :param payload: событие с текущим состоянием платежа
        :raises WebhookDeliveryError: уведомление не принято
        """
        try:
            response = await self._client.post(
                url,
                content=payload.model_dump_json(),
                headers={
                    'Content-Type': 'application/json',
                    'X-Event-Type': payload.event,
                    'X-Payment-Id': str(payload.payment.id),
                },
            )
        except httpx.HTTPError as exc:
            raise WebhookDeliveryError(f'{type(exc).__name__}: {exc}') from exc
        if not response.is_success:
            raise WebhookDeliveryError(
                f'Webhook endpoint responded with HTTP {response.status_code}'
            )
