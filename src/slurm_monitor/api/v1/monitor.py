# from slurm_monitor.backend.worker import celery_app
from fastapi import APIRouter, Depends, HTTPException
from logging import getLogger, Logger
from slurm_monitor.utils import utcnow
import slurm_monitor.db_operations as db_ops
from fastapi_cache.decorator import cache
from tqdm import tqdm

from slurm_monitor.utils.slurm import Slurm

logger: Logger = getLogger(__name__)

api_router = APIRouter(
    prefix="/monitor",
    tags=["monitor"],
)

NODE_INFOS = {}

def _get_slurmrestd(prefix: str):
    try:
        return Slurm.get_slurmrestd(prefix)
    except Exception as e:
        logger.warn(e)
        raise HTTPException(
                status_code=503,
                detail="The slurmrestd service seems to be down. SLURM or the server might be under maintenance"
        )

def _get_nodeinfo(nodelist: list[str] | None, dbi):
    if not nodelist:
        nodelist = dbi.get_nodes()

    gpu_nodelist = dbi.get_gpu_nodes()

    nodeinfo = {}
    for nodename in tqdm(sorted(nodelist)):
        nodeinfo[nodename] = {}
        if nodename in gpu_nodelist:
            try:
                nodeinfo[nodename].update(dbi.get_gpu_infos(node=nodename))
            except Exception as e:
                logger.warn(f"Internal error: Retrieving GPU info for {nodename} failed -- {e}")

        try:
            nodeinfo[nodename].update(dbi.get_cpu_infos(nodename) )
        except Exception as e:
            logger.warn(f"Internal error: Retrieving CPU info for {nodename} failed -- {e}")
    return nodeinfo


def validate_interval(end_time_in_s: float | None, start_time_in_s: float | None, resolution_in_s: int | None):
    if end_time_in_s is None:
        now = utcnow()
        end_time_in_s = now.timestamp()

    # Default 1h interval
    if start_time_in_s is None:
        start_time_in_s = end_time_in_s - 60 * 60.0

    if resolution_in_s is None:
        resolution_in_s = max(60, int((end_time_in_s - start_time_in_s) / 120))

    if end_time_in_s < start_time_in_s:
        raise HTTPException(
            status_code=500,
            detail=f"ValueError: {end_time_in_s=} cannot be smaller than {start_time_in_s=}",
        )

    if (end_time_in_s - start_time_in_s) > 3600*24*14:
        raise HTTPException(
            status_code=500,
            detail="ValueError: timeframe cannot exceed 14 days",
        )

    if resolution_in_s <= 0:
        raise HTTPException(
            status_code=500,
            detail=f"ValueError: {resolution_in_s=} must be >= 1 and <= 24*60*60",
        )

    return start_time_in_s, end_time_in_s, resolution_in_s

def load_node_infos() -> dict[str, any]:
    global NODE_INFOS
    dbi = db_ops.get_database()
    NODE_INFOS = _get_nodeinfo(None, dbi)
    return NODE_INFOS

@api_router.get("/jobs", response_model=None)
@cache(expire=30)
async def jobs():
    """
    Check status of jobs
    """
    return _get_slurmrestd("/jobs")


@api_router.get("/nodes", response_model=None)
@cache(expire=60)
async def nodes():
    """
    Check status of nodes
    """
    return _get_slurmrestd("/nodes")


@api_router.get("/nodes/{nodename}/info", response_model=None)
@cache(expire=3600*24)
async def nodes_nodename_info(nodename: str, dbi = Depends(db_ops.get_database)):
    global NODE_INFOS

    if not NODE_INFOS:
        load_node_infos()

    return NODE_INFOS[nodename]


@api_router.get("/partitions", response_model=None)
@cache(expire=3600)
async def partitions():
    """
    Check status of partitions
    """
    return _get_slurmrestd("/partitions")


@api_router.get("/nodes/info", response_model=None)
@cache(expire=3600*24)
async def nodes_info(nodes: list[str] | None = None, dbi=Depends(db_ops.get_database)):
    global NODE_INFOS

    if not NODE_INFOS:
        load_node_infos()

    return {'nodes': NODE_INFOS}

@api_router.get("/nodes/refresh_info", response_model=None)
async def nodes_refreshinfo():
    load_node_infos()
    return {'nodes': NODE_INFOS}


@api_router.get("/nodes/{nodename}/gpu_status")
@api_router.get("/nodes/gpustatus")
async def gpustatus(
    nodename: str | None = None,
    start_time_in_s: float | None = None,
    end_time_in_s: float | None = None,
    resolution_in_s: int | None = None,
    local_indices: str | None = None,
    dbi=Depends(db_ops.get_database),
):
    start_time_in_s, end_time_in_s, resolution_in_s = validate_interval(
            start_time_in_s=start_time_in_s,
            end_time_in_s=end_time_in_s,
            resolution_in_s=resolution_in_s
    )

    if local_indices is not None:
        local_indices = [int(x) for x in local_indices.split(',')]

    nodes = [] if nodename is None else [nodename]
    return {
        "gpu_status": dbi.get_gpu_status_timeseries_list(
            nodes=nodes,
            start_time_in_s=start_time_in_s,
            end_time_in_s=end_time_in_s,
            resolution_in_s=resolution_in_s,
            local_indices=local_indices,
        )
    }


@api_router.get("/job/{job_id}")
@api_router.get("/jobs/{job_id}/info")
async def job_status(
    job_id: int,
    start_time_in_s: float | None = None,
    end_time_in_s: float | None = None,
    resolution_in_s: int | None = None,
    dbi=Depends(db_ops.get_database),
):
    return {"job_status": dbi.get_job(job_id=job_id) }


@api_router.get("/jobs/{job_id}/system_status")
async def job_system_status(
    job_id: int,
    start_time_in_s: float | None = None,
    end_time_in_s: float | None = None,
    resolution_in_s: int | None = None,
    detailed: bool = False,
    dbi=Depends(db_ops.get_database),
):
    start_time_in_s, end_time_in_s, resolution_in_s = validate_interval(
            start_time_in_s=start_time_in_s,
            end_time_in_s=end_time_in_s,
            resolution_in_s=resolution_in_s
    )

    data = {}
    timeseries_per_node = dbi.get_job_status_timeseries_list(
            job_id=job_id,
            start_time_in_s=start_time_in_s,
            end_time_in_s=end_time_in_s,
            resolution_in_s=resolution_in_s,
            detailed=detailed
    )
    data["nodes"] = timeseries_per_node
    return data
