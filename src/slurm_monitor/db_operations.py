import functools
from logging import getLogger, Logger
import sqlalchemy

from fastapi import HTTPException
from slurm_monitor.app_settings import AppSettings
from slurm_monitor.db.db import SlurmMonitorDB

logger: Logger = getLogger(__name__)


@functools.cache
def get_database(app_settings: AppSettings | None = None):
    if app_settings is None:
        app_settings = AppSettings.get_instance()

    logger.info("Loading database with: {app_settings.database}")
    try:
        return SlurmMonitorDB(app_settings.database)
    except sqlalchemy.exc.OperationalError:
        raise HTTPException(
            status_code=500,
            detail=f"Cannot access monitor database - {app_settings.database.uri}",
        )
