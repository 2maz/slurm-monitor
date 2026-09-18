from typing import Annotated

from fastapi import Depends
from fastapi_cache.decorator import cache

from slurm_monitor.api.response_models import ClusterResponse
from slurm_monitor.api.routes import TokenPayload, api_router, get_token_payload
from slurm_monitor.db.db import ClusterDB
from slurm_monitor.db_operations import DBManager


@api_router.get(
    "/cluster",
    summary="Available clusters",
    tags=["cluster"],
    response_model=list[ClusterResponse],
)
@cache(expire=3600)
async def cluster(
    token_payload: Annotated[TokenPayload, Depends(get_token_payload)],
    time_in_s: int | None = None,
    dbi: ClusterDB = Depends(DBManager.get_database),
):
    """
    Get the list of clusters (available at a particular point in time)
    """
    return await dbi.get_clusters(time_in_s=time_in_s)
