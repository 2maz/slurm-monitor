from logging import getLogger, Logger
import sqlalchemy

from fastapi import HTTPException
from typing import ClassVar
from slurm_monitor.app_settings import AppSettings

logger: Logger = getLogger(__name__)

class DBManager:
    _databases: ClassVar[dict[str, any]] = {}

    @classmethod
    def get_database(cls, app_settings: AppSettings | None = None):
        if app_settings is None:
            app_settings = AppSettings.get_instance()

        logger.info(f"Loading database with: {app_settings.database}")

        try:
            if app_settings.db_schema_version == "v1":
                from slurm_monitor.db.v1.db import SlurmMonitorDB
                db = SlurmMonitorDB(app_settings.database)
            elif app_settings.db_schema_version == "v2":
                from slurm_monitor.db.v2.db import ClusterDB
                db = ClusterDB(app_settings.database)
            else:
                raise RuntimeError("AppSettings.db_schema_version is not set")

            return db
        except sqlalchemy.exc.OperationalError:
            raise HTTPException(
                status_code=500,
                detail=f"Cannot access monitor database - {app_settings.database.uri}",
            )
