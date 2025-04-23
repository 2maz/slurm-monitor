from logging import getLogger, Logger
import sqlalchemy

from fastapi import HTTPException
from slurm_monitor.app_settings import (
    AppSettings,
    AppSettingsV2
)
from slurm_monitor.db.v1.db import SlurmMonitorDB
from slurm_monitor.db.v2.db import ClusterDB

logger: Logger = getLogger(__name__)

databases = {}

def get_database(app_settings: AppSettings | None = None):
    if app_settings is None:
        app_settings = AppSettings.get_instance()

    logger.info(f"Loading database with: {app_settings.database}")
    try:
        return SlurmMonitorDB(app_settings.database)
    except sqlalchemy.exc.OperationalError:
        raise HTTPException(
            status_code=500,
            detail=f"Cannot access monitor database - {app_settings.database.uri}",
        )

def get_database_v2(app_settings: AppSettingsV2 | None = None):
    if app_settings is None:
        app_settings = AppSettingsV2.get_instance()

    if app_settings.database.uri not in databases:
        logger.info(f"Loading database with: {app_settings.database}")
        try:
            databases[app_settings.database.uri] = ClusterDB(app_settings.database)
        except sqlalchemy.exc.OperationalError:
            raise HTTPException(
                status_code=500,
                detail=f"Cannot access monitor database - {app_settings.database.uri}",
            )

    return databases[app_settings.database.uri]
