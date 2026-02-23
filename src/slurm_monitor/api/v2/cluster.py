from fastapi import Depends
from fastapi_cache.decorator import cache
from slurm_monitor.db_operations import DBManager
from slurm_monitor.db.v2.db import ClusterDB
from slurm_monitor.api.v2.routes import api_router, get_token_payload, TokenPayload
from slurm_monitor.api.v2.response_models import ClusterResponse

from typing import Annotated


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
