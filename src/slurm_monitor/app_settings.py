from __future__ import annotations
import hashlib
import json
from jwt import PyJWKClient
import os
import logging
from pathlib import Path
from pydantic import Field, computed_field, BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict
import sys

from slurm_monitor.db import DatabaseSettings

SLURM_MONITOR_LISTEN_PORT = 9099
SLURM_MONITOR_LISTEN_UI_PORT = 25052

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

class ServerSettings(BaseModel):
    host: str
    port: int

class ListenStatsSettings(BaseModel):
    interval: int = Field(default=30, description="Interval in seconds to compute stats")

class ListenSettings(BaseModel):
    cluster: str | None = Field(default=None, description="Name of cluster")
    lookback: int | None = Field(default=None, description="Lookback timeframe in hours")

    ui: ServerSettings = Field(default=ServerSettings(host="localhost", port=SLURM_MONITOR_LISTEN_UI_PORT), description="Connection to UI")
    kafka: ServerSettings  = Field(
            default=ServerSettings(host="localhost", port=SLURM_MONITOR_LISTEN_PORT),
            description="Connection to kafka broker",
    )

    stats: ListenStatsSettings = Field(default_factory=ListenStatsSettings)

class SSLSettings(BaseModel):
    keyfile: str | None = Field(default=None, description="Keyfile to use")
    certfile: str | None = Field(default=None, description="Certfile to use")

class AppSettings(BaseSettings):
    # export SLURM_MONITOR_ENVFILE='.dev.env' in order to change the default
    # .env file that is being loaded
    model_config = SettingsConfigDict(
                    env_file='.env',
                    env_nested_delimiter='_',
                    env_prefix='SLURM_MONITOR_',
                    extra='ignore'
                )
    host: str = Field(default="0.0.0.0")
    port: int = Field(default=12000)
    ssl: SSLSettings = Field(default_factory=SSLSettings)

    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    data_dir: str | None = Field(default=None)

    prefetch: PrefetchSettings = Field(default_factory=PrefetchSettings)

    db_schema_version: str | None = Field(default="v2")
    oauth: OAuthSettings = Field(default_factory=OAuthSettings)

    listen: ListenSettings = Field(default_factory=ListenSettings)

    @classmethod
    def get_instance(cls) -> AppSettings:
        if not hasattr(cls, "_instance") or not cls._instance:
            raise RuntimeError(
                "AppSettings: instance is not accessible. Please call AppSettings.initialize() first."
            )

        return cls._instance

    @classmethod
    def initialize(cls, force: bool = False, env_file_required: bool = False, **kwargs) -> AppSettings:
        if not force and hasattr(cls, "_instance") and cls._instance:
            return cls._instance

        if env_file_required:
            env_file = ".env"
            if "--env-file" in sys.argv:
                idx = sys.argv.index("--env-file")
                env_file = sys.argv[idx + 1]

            if "SLURM_MONITOR_ENVFILE" in os.environ:
                env_file = os.environ["SLURM_MONITOR_ENVFILE"]

            if env_file == "" or not Path(env_file).exists():
                raise FileNotFoundError(
                    f"AppSettings.initialize: could not find {env_file=}"
                )

            logger.info(f"AppSettings.initialize: loading {env_file=}")
            cls._instance = AppSettings(_env_file=env_file, **kwargs)
        else:
            cls._instance = AppSettings(**kwargs)

        if cls._instance.data_dir:
            cls._instance.data_dir = str(Path(cls._instance.data_dir).resolve())
        return cls._instance

    def hexdigest(self) -> str:
        """
        Return hexdigest of hashed object
        """
        txt = json.dumps(self.model_dump(), sort_keys=True, default=str)
        return hashlib.sha256(txt.encode('UTF-8')).hexdigest()
