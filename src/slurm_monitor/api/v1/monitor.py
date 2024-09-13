# from slurm_monitor.backend.worker import celery_app
from fastapi import APIRouter, Depends, HTTPException
from logging import getLogger, Logger
import subprocess
import json
import yaml
from slurm_monitor.utils import utcnow
import slurm_monitor.db_operations as db_ops
from fastapi_cache.decorator import cache
from pathlib import Path
from tqdm import tqdm

from slurm_monitor.slurm import Slurm

logger: Logger = getLogger(__name__)

api_router = APIRouter(
    prefix="/monitor",
    tags=["monitor"],
)

NODE_INFOS = {}
NODE_INFOS_FILENAME="/tmp/slurm-monitor/nodeinfo.yaml"

Slurm.ensure_restd()

def _get_slurmrestd(prefix: str):
    try:
        return Slurm.get_slurmrestd(prefix)
    except Exception:
        raise HTTPException(
                status_code=503,
                detail="The slurmrestd service seems to be down. SLURM or the server might be under maintenance"
        )

def _get_cpu_infos(node: str, user: str = Slurm.get_user()):
    msg = f"ssh -oBatchMode=yes -l {user} {node} lscpu | sed -rn '/Model name/ s/.*:\s*(.*)/\\1/p'"
    cpus = {}
    try:
        response = subprocess.run(msg, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        error_message = response.stderr.decode("utf-8").strip()
        if error_message:
            cpus["error"] = error_message
            logger.warning(f"Host is not reachable - {error_message}")
        else:
            cpus["model_name"] = response.stdout.decode("utf-8").strip()
    except Exception as e:
        logger.warn(e)

    return { "cpus": cpus }


def _get_nodeinfo(nodelist: list[str] | None, dbi):
    if not nodelist:
        nodelist = Slurm.get_node_names()

    gpu_nodelist = [x.name for x in dbi.get_nodes()]

    nodeinfo = {}
    import pdb
    pdb.set_trace()
    for nodename in tqdm(nodelist):
        nodeinfo[nodename] = {}
        if nodename in gpu_nodelist:
            try:
                nodeinfo[nodename].update(dbi.get_gpu_infos(node=nodename))
            except Exception as e:
                logger.warn(f"Internal error: Retrieving GPU info for {nodename} failed -- {e}")

        try:
            nodeinfo[nodename].update( _get_cpu_infos(nodename) )
        except Exception as e:
            logger.warn(f"Internal error: Retrieving CPU info for {nodename} failed -- {e}")
    return nodeinfo

def load_node_infos(refresh: bool = False):
    global NODE_INFOS
    node_info_path = Path(NODE_INFOS_FILENAME)
    if not refresh and node_info_path.exists():
        with open(node_info_path, "r") as f:
            NODE_INFOS = yaml.safe_load(f)
    else:
        dbi = db_ops.get_database()
        NODE_INFOS = _get_nodeinfo(None, dbi)

        node_info_path.parent.mkdir(parents=True, exist_ok=True)
        with open(node_info_path, "w") as f:
            yaml.dump(NODE_INFOS, f)

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
async def node_gpuinfo(nodename: str, dbi = Depends(db_ops.get_database)):
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
async def nodeinfo(nodes: list[str] | None = None, dbi=Depends(db_ops.get_database)):
    return {'nodes': NODE_INFOS}

@api_router.get("/nodes/refresh_info", response_model=None)
async def nodeinfo():
    load_node_infos()
    return {'nodes': NODE_INFOS} 


@api_router.get("/gpustatus")
@cache(expire=3600)
async def gpustatus(
    node: str | None = None,
    start_time_in_s: float | None = None,
    end_time_in_s: float | None = None,
    resolution_in_s: int | None = None,
    local_indices: str | None = None,
    dbi=Depends(db_ops.get_database),
):
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

    if resolution_in_s <= 0:
        raise HTTPException(
            status_code=500,
            detail=f"ValueError: {resolution_in_s=} must be >= 1 and <= 24*60*60",
        )

    if local_indices is not None:
        local_indices = [int(x) for x in local_indices.split(',')]

    return {
        "gpu_status": dbi.get_gpu_status_timeseries_list(
            nodes=[node],
            start_time_in_s=start_time_in_s,
            end_time_in_s=end_time_in_s,
            resolution_in_s=resolution_in_s,
            local_indices=local_indices,
        )
    }


@api_router.get("/job/{job_id}")
async def job_status(
    job_id: int,
    dbi=Depends(db_ops.get_database),
):
    return {"job_status": dbi.get_job(job_id=job_id)}
