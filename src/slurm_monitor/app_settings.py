from __future__ import annotations
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from slurm_monitor.db import DatabaseSettings


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(
                    env_nested_delimiter='_',
                    env_prefix='SLURM_MONITOR_'
                )

    database: DatabaseSettings = Field(default_factory=DatabaseSettings)

    @classmethod
    def get_instance(cls) -> AppSettings:
        if not cls._instance:
            raise RuntimeError(
                "AppSettings: instance is not accessible. Please call AppSettings.initialize() first."
            )

        return cls._instance

    @classmethod
    def initialize(cls) -> None:
        cls._instance = AppSettings()
