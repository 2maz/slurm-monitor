from fastapi import Request, Depends
from fastapi_cache.decorator import cache
import slurm_monitor.db_operations as db_ops
from slurm_monitor.api.v2.routes import api_router, get_token_payload, TokenPayload
from slurm_monitor.api.v2.response_models import ClusterResponse

from typing import Annotated


@api_router.get("/cluster",
        summary="Available clusters",
        tags=["cluster"],
        response_model=list[ClusterResponse]
)
@cache(expire=3600)
async def cluster(token_payload: Annotated[TokenPayload, Depends(get_token_payload)], time_in_s: int | None = None):
    """
    Get the list of clusters (available at a particular point in time)
    """
    dbi = db_ops.get_database()
    return await dbi.get_clusters(time_in_s=time_in_s)
