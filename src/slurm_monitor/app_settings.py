from __future__ import annotations
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from slurm_monitor.db import DatabaseSettings


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(
                    env_file='.env',
                    env_nested_delimiter='_',
                    env_prefix='SLURM_MONITOR_'
                )
    host: str = Field(default="localhost")
    port: int = Field(default=12000)

    database: DatabaseSettings = Field(default_factory=DatabaseSettings)

    @classmethod
    def get_instance(cls) -> AppSettings:
        if not cls._instance:
            raise RuntimeError(
                "AppSettings: instance is not accessible. Please call AppSettings.initialize() first."
            )

        return cls._instance

    @classmethod
    def initialize(cls) -> AppSettings:
        cls._instance = AppSettings()
        return cls._instance
