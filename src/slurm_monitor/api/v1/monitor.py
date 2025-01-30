# from slurm_monitor.backend.worker import celery_app
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response, FileResponse
from fastapi_cache.decorator import cache
from logging import getLogger, Logger
import pandas as pd
from pathlib import Path
import re
import tempfile
from tqdm import tqdm

from slurm_monitor.utils import utcnow
import slurm_monitor.db_operations as db_ops
from slurm_monitor.utils.command import Command
from slurm_monitor.utils.slurm import Slurm
from slurm_monitor.db.v1.query import QueryMaker
from slurm_monitor.app_settings import AppSettings

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


@api_router.get("/nodes/{nodename}/topology", response_model=None)
@cache(expire=3600*24*7)
def nodes_nodename_topology(nodename: str, output_format: str = 'svg'):
    filename = Path(tempfile.gettempdir()) / f"slurm-monitor-lstopo-{nodename}.{output_format}"
    if not Path(filename).exists():
        user = Command.get_user()

        decode = None if output_format == 'png' else 'UTF-8'
        write = "wb" if output_format == 'png' else 'w'

        # system setup for user must ensure that lstopo is available, e.g., adding
        #     module load hwloc
        # into ~/.profile
        data = Command.run(
                f"ssh {user}@{nodename} \"bash -l -c 'lstopo --output-format {output_format} -v'\"",
                decode=decode
               )
        if output_format == 'svg':
            # fixing viewport - should not have units
            data = re.sub(r"viewBox='0 0 ([0-9]+)px ([0-9]+)px'", "viewBox='0 0 \\1 \\2'", data)

        with open(filename, write) as f:
            f.write(data)

    if output_format == 'png':
        return FileResponse(filename)

    with open(filename, "r") as f:
        data = f.read()

    if output_format == 'svg':
        return Response(content=data, media_type="image/svg+xml")
    else:
        return data


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

@api_router.get("/nodes/last_probe_timestamp", response_model=None)
async def nodes_last_probe_timestamp():
    dbi = db_ops.get_database()
    return await dbi.get_last_probe_timestamp()

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
        "gpu_status": await dbi.get_gpu_status_timeseries_list(
            nodes=nodes,
            start_time_in_s=start_time_in_s,
            end_time_in_s=end_time_in_s,
            resolution_in_s=resolution_in_s,
            local_indices=local_indices,
        )
    }

@api_router.get("/nodes/{nodename}/cpu_status")
@api_router.get("/nodes/cpu_status")
async def cpu_status(
    nodename: str | None = None,
    start_time_in_s: float | None = None,
    end_time_in_s: float | None = None,
    resolution_in_s: int | None = None,
    dbi=Depends(db_ops.get_database),
):
    start_time_in_s, end_time_in_s, resolution_in_s = validate_interval(
            start_time_in_s=start_time_in_s,
            end_time_in_s=end_time_in_s,
            resolution_in_s=resolution_in_s
    )

    nodes = [] if nodename is None else [nodename]
    return {
        "cpu_status": await dbi.get_cpu_status_timeseries_list(
            nodes=nodes,
            start_time_in_s=start_time_in_s,
            end_time_in_s=end_time_in_s,
            resolution_in_s=resolution_in_s,
        )
    }

@api_router.get("/nodes/{nodename}/memory_status")
@api_router.get("/nodes/memory_status")
async def memory_status(
    nodename: str | None = None,
    start_time_in_s: float | None = None,
    end_time_in_s: float | None = None,
    resolution_in_s: int | None = None,
    dbi=Depends(db_ops.get_database),
):
    start_time_in_s, end_time_in_s, resolution_in_s = validate_interval(
            start_time_in_s=start_time_in_s,
            end_time_in_s=end_time_in_s,
            resolution_in_s=resolution_in_s
    )

    nodes = [] if nodename is None else [nodename]
    return {
        "memory_status": await dbi.get_memory_status_timeseries_list(
            nodes=nodes,
            start_time_in_s=start_time_in_s,
            end_time_in_s=end_time_in_s,
            resolution_in_s=resolution_in_s,
        )
    }

@api_router.get("/jobs/query")
async def jobs_query(
    user: str | None = None,
    user_id: int | None = None,
    job_id: int | None = None,
    start_before_in_s: float | None = None,
    start_after_in_s: float | None = None,
    end_before_in_s: float | None = None,
    end_after_in_s: float | None = None,
    submit_before_in_s: float | None = None,
    submit_after_in_s: float | None = None,
    min_duration_in_s: float | None = None,
    max_duration_in_s: float | None = None,
    limit: int = 100,
):
    dbi = db_ops.get_database()
    return {"jobs": await dbi.get_jobs(
        user=user,
        user_id=user_id,
        job_id=job_id,
        start_before_in_s=start_before_in_s,
        start_after_in_s=start_after_in_s,
        end_before_in_s=end_before_in_s,
        end_after_in_s=end_after_in_s,
        submit_before_in_s=submit_before_in_s,
        submit_after_in_s=submit_after_in_s,
        min_duration_in_s=min_duration_in_s,
        max_duration_in_s=max_duration_in_s,
        limit=limit
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
    return {"job_status": await dbi.get_job(job_id=job_id) }


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
    timeseries_per_node = await dbi.get_job_status_timeseries_list(
            job_id=job_id,
            start_time_in_s=start_time_in_s,
            end_time_in_s=end_time_in_s,
            resolution_in_s=resolution_in_s,
            detailed=detailed
    )
    data["nodes"] = timeseries_per_node
    return data

@api_router.get("/queries")
@api_router.get("/queries/{query_name}")
async def queries(
    query_name: str = None,
    start_time_in_s: float | None = None,
    end_time_in_s: float | None = None,
):
    if query_name is None:
        return { 'queries': QueryMaker.list_available() }

    try:
        query_maker = QueryMaker(db_ops.get_database())
        query = query_maker.create(query_name)
        df = await query.execute_async()

        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(
                status_code=404,
                detail=f"Failed to execute query: '{query_name}' -- {e}"
        )


@api_router.get("/benchmarks/{benchmark_name}")
def benchmarks(benchmark_name: str = "lambdal"):
    app_settings = AppSettings.initialize()
    if app_settings.data_dir is None:
        return {}

    path = Path(app_settings.data_dir) / f"{benchmark_name}-benchmark-results.csv"
    if not path.exists():
        return {}

    df = pd.read_csv(str(path))
    df = df.fillna(-1)
    del df['Unnamed: 0']
    return df.to_dict(orient="records")
