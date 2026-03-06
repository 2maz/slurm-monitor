# from slurm_monitor.backend.worker import celery_app
from fastapi import Depends, HTTPException, status
from logging import getLogger, Logger
from typing import Annotated

from slurm_monitor.app_settings import AppSettings
from slurm_monitor.db_operations import DBManager
from slurm_monitor.api.v2.routes import (
    api_router,
    get_token_payload,
    TokenPayload
)

from fastapi.encoders import jsonable_encoder



logger: Logger = getLogger(__name__)

@api_router.get("/user/settings",
        summary="Get user settings",
        tags=["user"]
)
def get_settings(
        token_payload: Annotated[TokenPayload, Depends(get_token_payload)],
        dbi = Depends(DBManager.get_database)
        ):
    """
    Get user settings
    """
    app_settings = AppSettings.get_instance()
    if not app_settings.oauth.required:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access to settings required oauth to be active on the backend"
        )

    return dbi.get_user_settings(user=token_payload.preferred_username)


@api_router.put("/user/settings",
        summary="Create user settings",
        tags=["user"]
)
@api_router.post("/user/settings",
        summary="Create user settings",
        tags=["user"]
)
def set_settings(
        token_payload: Annotated[TokenPayload, Depends(get_token_payload)],
        settings,
        dbi = Depends(DBManager.get_database)
        ):
    """
    Post / Set the user settings
    """
    app_settings = AppSettings.get_instance()
    if not app_settings.oauth.required:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access to settings required oauth to be active on the backend"
        )
    settings = jsonable_encoder(settings)
    return dbi.set_user_settings(user=token_payload.preferred_username, settings=settings)
