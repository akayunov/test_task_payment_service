from functools import lru_cache
from typing import Literal, Self

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore',
        case_sensitive=False,
    )

    app_name: str = 'Payment Service'
    app_version: str = '0.1.0'
    environment: Literal['local', 'test', 'production'] = 'local'
    debug: bool = False
    api_prefix: str = '/api/v1'
    log_level: str = 'INFO'

    database_url: str
    db_pool_size: int = 5
    db_max_overflow: int = 10
    db_echo: bool = False

    rabbitmq_url: str = 'amqp://guest:guest@localhost:5672/'
    rabbitmq_prefetch_count: int = Field(default=10, ge=1)

    api_key: str = Field(min_length=16)

    outbox_poll_interval: float = Field(default=1.0, gt=0)
    outbox_batch_size: int = Field(default=100, ge=1, le=1000)

    gateway_min_delay: float = Field(default=2.0, ge=0)
    gateway_max_delay: float = Field(default=5.0, ge=0)
    gateway_success_rate: float = Field(default=0.9, ge=0, le=1)

    retry_max_attempts: int = Field(default=3, ge=1, le=10)
    retry_base_delay: float = Field(default=2.0, gt=0)
    webhook_timeout: float = Field(default=10.0, gt=0)

    @field_validator('database_url')
    @classmethod
    def database_url_must_be_asyncpg(cls, value: str) -> str:
        if not value.startswith('postgresql+asyncpg://'):
            raise ValueError('database_url must start with postgresql+asyncpg://')
        return value

    @model_validator(mode='after')
    def check_consistency(self) -> Self:
        if self.gateway_min_delay > self.gateway_max_delay:
            raise ValueError('gateway_min_delay must not exceed gateway_max_delay')
        if self.environment == 'production':
            if self.debug:
                raise ValueError('debug must be disabled in production')
            if 'dev-only' in self.api_key:
                raise ValueError('API_KEY must be set explicitly in production')
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
