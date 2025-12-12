from __future__ import annotations
from jwt import PyJWKClient
import os
import logging
from pathlib import Path
from pydantic import Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict

from slurm_monitor.db import DatabaseSettings

logger = logging.getLogger(__name__)


class OAuthSettings(BaseSettings):
    required: bool = Field(default=False)

    client: str = Field(default='')
    client_secret: str = Field(default='')
    url: str = Field(default='')
    realm: str = Field(default='')

    @computed_field
    @property
    def issuer(self) -> str:
        return f"{self.url}/realms/{self.realm}"

    @computed_field
    @property
    def jwks_url(self) -> str:
        return f"{self.issuer}/protocol/openid-connect/certs"

    @computed_field
    @property
    def jwks_client(self) -> PyJWKClient:
        if not hasattr(self, '_jwks_client'):
            self._jwks_client = PyJWKClient(self.jwks_url)
        return self._jwks_client

class PrefetchSettings(BaseSettings):
    enabled: bool = Field(default=True)
    interval: int = Field(default=90)

class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(
                    env_file='.env',
                    env_nested_delimiter='_',
                    env_prefix='SLURM_MONITOR_',
                    extra='ignore'
                )
    host: str = Field(default="localhost")
    port: int = Field(default=12000)

    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    data_dir: str | None = Field(default=None)

    prefetch: PrefetchSettings = Field(default_factory=PrefetchSettings)

    db_schema_version: str | None = Field(default=None)
    oauth: OAuthSettings = Field(default_factory=OAuthSettings)

    @classmethod
    def get_instance(cls) -> AppSettings:
        if not hasattr(cls, "_instance") or not cls._instance:
            raise RuntimeError(
                "AppSettings: instance is not accessible. Please call AppSettings.initialize() first."
            )

        return cls._instance

    @classmethod
    def initialize(cls) -> AppSettings:
        env_file = ".env"
        if "SLURM_MONITOR_ENVFILE" in os.environ:
            env_file = os.environ["SLURM_MONITOR_ENVFILE"]
            if not Path(env_file).exists():
                raise FileNotFoundError(f"AppSettings.initialize: could not find {env_file=}")

        logger.info(f"AppSettings.initialize: loading {env_file=}")
        cls._instance = AppSettings(_env_file=env_file)
        if cls._instance.data_dir:
            cls._instance.data_dir = str(Path(cls._instance.data_dir).resolve())
        return cls._instance
