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

from slurm_monitor.utils.command import Command
from slurm_monitor.utils.slurm import Slurm

from slurm_monitor.app_settings import AppSettingsV2
import slurm_monitor.db_operations as db_ops

logger: Logger = getLogger(__name__)

api_router = APIRouter(
#    prefix="",
    tags=["v2"]
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


@api_router.get("/{cluster}/nodes/{nodename}/info", response_model=None)
@cache(expire=3600*24)
async def nodes_nodename_info(cluster: str, nodename: str):
    dbi = db_ops.get_database_v2()
    return await dbi.get_nodes_info(cluster, nodename)


@api_router.get("/{cluster}/nodes", response_model=None)
@api_router.get("/{cluster}/nodes/info", response_model=None)
@cache(expire=3600*24)
async def nodes_info(cluster: str, nodes: list[str] | None = None, dbi=Depends(db_ops.get_database_v2)):
    return await dbi.get_nodes_info(cluster)


@api_router.get("/{cluster}/nodes/{nodename}/topology", response_model=None)
async def nodes_nodename_topology(cluster: str, nodename: str):
    dbi = db_ops.get_database_v2()
    node_config = await dbi.get_nodes_info(cluster, nodename)
    data = node_config[nodename].get('topo_svg', None)
    if data:
        return Response(content=data, media_type="image/svg+xml")
    else:
        raise HTTPException(status_code=500,
                detail=f"No topology information available for {nodename=} {cluster=}")



####### v1 ###############################
@api_router.get("/jobs", response_model=None)
@cache(expire=30)
async def jobs():
    """
    Check status of jobs
    """
    return _get_slurmrestd("/jobs")



@api_router.get("/{cluster}/partitions", response_model=None)
@cache(expire=3600)
async def partitions():
    """
    Check status of partitions
    """
    return _get_slurmrestd("/partitions")


@api_router.get("{cluster}/nodes/last_probe_timestamp", response_model=None)
async def nodes_last_probe_timestamp(cluster: str):
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


@api_router.get("/jobs/{job_id}/export")
async def job_export(
        job_id: int,
        refresh: bool = False
) -> FileResponse:
    dbi = db_ops.get_database()
    zip_filename = Path(f"{dbi.get_export_data_dirname(job_id=job_id)}.zip")
    if refresh or not zip_filename.exists():
        zip_filename = await dbi.export_data(job_id=job_id)

    logger.info(f"Job Data Export: {zip_filename}")
    return FileResponse(path=zip_filename, filename=f"job-data-{job_id}.zip")


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
