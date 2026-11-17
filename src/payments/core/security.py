import secrets

from payments.core.config import get_settings
from payments.core.exceptions import UnauthorizedError


def verify_api_key(api_key: str | None) -> None:
    """Сверить ключ за постоянное время.

    :param api_key: значение X-API-Key или None
    :raises UnauthorizedError: ключ отсутствует или не совпал
    """
    if api_key is None:
        raise UnauthorizedError('X-API-Key header is required', code='api_key_missing')
    expected = get_settings().api_key
    if not secrets.compare_digest(api_key.encode(), expected.encode()):
        raise UnauthorizedError('Invalid API key', code='api_key_invalid')
