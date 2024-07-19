# from slurm_monitor.backend.worker import celery_app
from fastapi import APIRouter, Depends, HTTPException
from logging import getLogger, Logger
import subprocess
import json
from slurm_monitor.utils import utcnow
import slurm_monitor.db_operations as db_ops
from fastapi_cache.decorator import cache

logger: Logger = getLogger(__name__)

SLURM_API_PREFIX = "/slurm/v0.0.37"

api_router = APIRouter(
    prefix="/monitor",
    tags=["monitor"],
)


def get_user():
    return (
        subprocess.run("whoami", stdout=subprocess.PIPE).stdout.decode("utf-8").strip()
    )


def _get_slurmrestd(prefix: str):
    if not prefix.startswith("/"):
        prefix = f"/{prefix}"

    msg = f'echo -e "GET {SLURM_API_PREFIX}{prefix} HTTP/1.1\r\n" | slurmrestd -a rest_auth/local'
    logger.debug(f"Query: {msg}")
    response = subprocess.run(msg, shell=True, stdout=subprocess.PIPE).stdout.decode(
        "utf-8"
    )
    header, content = response.split("\r\n\r\n", 1)
    json_data = json.loads(content)
    logger.debug(f"Response: {json_data}")
    return json_data


def _get_gpuinfo(nodelist: list[str] | None = None):
    if nodelist is None:
        return {}

    gpuinfo = {}
    for nodename in nodelist:
        gpuinfo[nodename] = _get_gpuinfo_for_node(nodename=nodename, user=get_user())

    return gpuinfo


def _get_gpuinfo_for_node(nodename: str, user: str):
    query_properties = [
        "gpu_name",
        "uuid",
        "pstate",
        "temperature.gpu",
        "power.draw",
        "utilization.gpu",
        "utilization.memory",
        "memory.used",
        "memory.free",
    ]

    msg = f"ssh {user}@{nodename} 'nvidia-smi --query-gpu={','.join(query_properties)} --format=csv,nounits,noheader'"
    response = subprocess.run(msg, shell=True, stdout=subprocess.PIPE).stdout.decode(
        "utf-8"
    )
    gpus = []
    for gpu_idx, line in enumerate(response.strip().split("\n")):
        gpu_data = {"index": gpu_idx}
        for idx, field in enumerate(line.split(",")):
            value = field.strip()
            gpu_data[query_properties[idx]] = value
        gpus.append(gpu_data)
    return {"gpus": gpus}


@api_router.get("/jobs", response_model=None)
@cache(expire=2)
async def jobs():
    """
    Check status of jobs
    """
    return _get_slurmrestd("/jobs")


@api_router.get("/nodes", response_model=None)
@cache(expire=2)
async def nodes():
    """
    Check status of nodes
    """
    return _get_slurmrestd("/nodes")


@api_router.get("/nodes/{nodename}/gpuinfo", response_model=None)
@cache(expire=30)
async def node_gpuinfo(nodename: str):
    return _get_gpuinfo_for_node(nodename=nodename, user=get_user())


@api_router.get("/partitions", response_model=None)
@cache(expire=2)
async def partitions():
    """
    Check status of partitions
    """
    return _get_slurmrestd("/partitions")


@api_router.get("/gpuinfo", response_model=None)
@cache(expire=30)
async def gpuinfo():
    return _get_gpuinfo(nodelist=["g001", "g002"])


@api_router.get("/gpustatus")
async def gpustatus(
    node: str | None = None,
    start_time_in_s: float | None = None,
    end_time_in_s: float | None = None,
    resolution_in_s: int | None = None,
    dbi=Depends(db_ops.get_database),
):
    if end_time_in_s is None:
        now = utcnow()
        end_time_in_s = now.timestamp()

    # Default 1h interval
    if start_time_in_s is None:
        start_time_in_s = end_time_in_s - 60 * 60.0

    if resolution_in_s is None:
        resolution_in_s = 30

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

    return {
        "gpu_status": dbi.get_gpu_status_timeseries_list(
            nodes=[node],
            start_time_in_s=start_time_in_s,
            end_time_in_s=end_time_in_s,
            resolution_in_s=resolution_in_s,
        )
    }


@api_router.get("/job/{job_id}")
async def job_status(
    job_id: int,
    dbi=Depends(db_ops.get_database),
):
    return {"job_status": dbi.get_job(job_id=job_id)}
