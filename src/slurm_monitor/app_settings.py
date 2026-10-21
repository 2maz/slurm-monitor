from __future__ import annotations

import hashlib
import json
import logging
import os
import sys
from pathlib import Path

from jwt import PyJWKClient
from pydantic import BaseModel, Field, computed_field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from slurm_monitor.db.settings import DatabaseSettings

SLURM_MONITOR_RESTAPI_PORT = 12000
SLURM_MONITOR_LISTEN_PORT = 9099
SLURM_MONITOR_LISTEN_UI_PORT = 25052

logger = logging.getLogger(__name__)


class OAuthSettings(BaseSettings):
    required: bool = Field(default=False)

    client: str = Field(default="")
    client_secret: str = Field(default="")
    url: str = Field(default="")
    realm: str = Field(default="")
    # Where to fetch the signing keys, when the issuer's own certs endpoint is
    # not reachable from the API server (e.g. the issuer is a public URL and
    # the identity provider is reached on an internal address). One word on
    # purpose: env_nested_delimiter="_" splits SLURM_MONITOR_OAUTH_JWKS_URL
    # into oauth.jwks.url, so the variable is SLURM_MONITOR_OAUTH_JWKS.
    jwks: str = Field(default="")
    # Accepted values of the token's "aud" claim, comma-separated
    # (SLURM_MONITOR_OAUTH_AUDIENCE). Keycloak puts "account" in the tokens of
    # clients that set no audience, hence the default; it is shared by every
    # client in the realm, so a deployment that has tokens issued for
    # slurm-monitor should name its own client here.
    audience: str = Field(default="account")

    @field_validator("audience")
    @classmethod
    def audience_not_empty(cls, value: str) -> str:
        if not any(part.strip() for part in value.split(",")):
            raise ValueError("at least one audience is required")
        return value

    @computed_field
    @property
    def audiences(self) -> list[str]:
        return [part.strip() for part in self.audience.split(",") if part.strip()]

    @computed_field
    @property
    def issuer(self) -> str:
        return f"{self.url}/realms/{self.realm}"

    @computed_field
    @property
    def jwks_url(self) -> str:
        return self.jwks or f"{self.issuer}/protocol/openid-connect/certs"

    @computed_field
    @property
    def jwks_client(self) -> PyJWKClient:
        if not hasattr(self, "_jwks_client"):
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

    ui: ServerSettings = Field(
        default=ServerSettings(host="localhost", port=SLURM_MONITOR_LISTEN_UI_PORT), description="Connection to UI"
    )
    kafka: ServerSettings = Field(
        default=ServerSettings(host="localhost", port=SLURM_MONITOR_LISTEN_PORT),
        description="Connection to kafka broker",
    )

    stats: ListenStatsSettings = Field(default_factory=ListenStatsSettings)
    retry: int = 15


class SSLSettings(BaseModel):
    keyfile: str | None = Field(default=None, description="Keyfile to use")
    certfile: str | None = Field(default=None, description="Certfile to use")


class AppSettings(BaseSettings):
    # export SLURM_MONITOR_ENVFILE='.dev.env' in order to change the default
    # .env file that is being loaded
    model_config = SettingsConfigDict(
        env_file=".env",
        env_nested_delimiter="_",
        env_prefix="SLURM_MONITOR_",
        extra="ignore",
    )
    host: str = Field(default="0.0.0.0")
    port: int = Field(default=SLURM_MONITOR_RESTAPI_PORT)
    ssl: SSLSettings = Field(default_factory=SSLSettings)

    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    data_dir: str | None = Field(default=None)

    prefetch: PrefetchSettings = Field(default_factory=PrefetchSettings)

    oauth: OAuthSettings = Field(default_factory=OAuthSettings)

    listen: ListenSettings = Field(default_factory=ListenSettings)

    @classmethod
    def get_instance(cls) -> AppSettings:
        if not hasattr(cls, "_instance") or not cls._instance:
            raise RuntimeError(
                "AppSettings: instance is not accessible. Please call AppSettings.initialize() first.",
            )

        return cls._instance

    @classmethod
    def initialize(cls, force: bool = False, env_file_required: bool = False, **kwargs) -> AppSettings:
        if not force and hasattr(cls, "_instance") and cls._instance:
            return cls._instance

        env_file = None
        if "SLURM_MONITOR_ENVFILE" in os.environ:
            env_file = os.environ["SLURM_MONITOR_ENVFILE"]
            if not Path(env_file).exists():
                raise FileNotFoundError(
                    f"AppSettings.initialize: could not find {env_file=} set via env SLURM_MONITOR_ENVFILE",
                )

        if "--env-file" in sys.argv:
            idx = sys.argv.index("--env-file")
            env_file = sys.argv[idx + 1]

            if not Path(env_file).exists():
                raise FileNotFoundError(
                    f"AppSettings.initialize: could not find {env_file=} provided via --env-file",
                )

        if env_file_required and not env_file:
            env_file = ".env"

        if env_file:
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
        return hashlib.sha256(txt.encode("UTF-8")).hexdigest()
