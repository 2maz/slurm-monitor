from __future__ import annotations
from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from slurm_monitor.db import DatabaseSettings


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

    db_schema_version: str | None = Field(default=None)
    oauth_required: bool = Field(default=False)

    @classmethod
    def get_instance(cls) -> AppSettings:
        if not hasattr(cls, "_instance") or not cls._instance:
            raise RuntimeError(
                "AppSettings: instance is not accessible. Please call AppSettings.initialize() first."
            )

        return cls._instance

    @classmethod
    def initialize(cls) -> AppSettings:
        cls._instance = AppSettings()
        if cls._instance.data_dir:
            cls._instance.data_dir = str(Path(cls._instance.data_dir).resolve())
        return cls._instance
