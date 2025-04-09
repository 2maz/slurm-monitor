# from slurm_monitor.backend.worker import celery_app
import asyncio
import datetime as dt
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response, FileResponse
from fastapi_cache.decorator import cache
from logging import getLogger, Logger
import pandas as pd
from pathlib import Path
import re
import tempfile
from tqdm import tqdm
import traceback

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
            detail=f"""ValueError: query timeframe cannot exceed 14 days (job length), but was
                {(end_time_in_s - start_time_in_s) / (3600*24):.2f} days""",
        )

    if resolution_in_s <= 0:
        raise HTTPException(
            status_code=500,
            detail=f"ValueError: {resolution_in_s=} must be >= 1 and <= 24*60*60",
        )

    return start_time_in_s, end_time_in_s, resolution_in_s


#### Results sorted by nodes

@api_router.get("/{cluster}/nodes/{nodename}/info", response_model=None)
@cache(expire=3600*24)
async def nodes_nodename_info(cluster: str, nodename: str):
    dbi = db_ops.get_database_v2()
    return await dbi.get_nodes_info(cluster, nodename)


@api_router.get("/{cluster}/nodes", response_model=None)
@api_router.get("/{cluster}/nodes/info", response_model=None)
@api_router.get("/{cluster}/nodes/{nodename}/info", response_model=None)
@cache(expire=3600*24)
async def nodes_info(cluster: str,
        nodename: str | None = None,
        time_in_s: int | None = None,
        dbi=Depends(db_ops.get_database_v2)):
    return await dbi.get_nodes_info(cluster, nodename, time_in_s)

@api_router.get("/{cluster}/nodes/states", response_model=None)
@api_router.get("/{cluster}/nodes/{nodename}/states", response_model=None)
async def nodes_states(cluster: str, nodename: str | None = None,
        time_in_s: int | None = None,
        ):
    dbi = db_ops.get_database_v2()
    return await dbi.get_nodes_states(cluster, nodename, time_in_s)


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


@api_router.get("/{cluster}/nodes/last_probe_timestamp", response_model=dict[str, dt.datetime])
async def nodes_last_probe_timestamp(cluster: str):
    """
    Retrieve the last the timestamps of
    """
    dbi = db_ops.get_database_v2()
    return await dbi.get_last_probe_timestamp(cluster=cluster)


@api_router.get("/{cluster}/nodes/{nodename}/gpu_util", description="test desc")
@api_router.get("/{cluster}/nodes/gpu_util", description="test desc")
async def nodes_process_gpu_util(
    cluster: str,
    nodename: str | None = None,
    reference_time_in_s: float | None = None,
    window_in_s: int | None = None,
):
    dbi = db_ops.get_database_v2()

    nodes = [nodename] if nodename else (await dbi.get_nodes(cluster))
    tasks = {}
    for node in nodes:
        tasks[node] = asyncio.create_task(dbi.get_node_sample_process_gpu_util(
                cluster=cluster,
                node=node,
                reference_time_in_s=reference_time_in_s,
                window_in_s=window_in_s
            )
        )
    return { x: (await task) for x, task in tasks.items()}

@api_router.get("/{cluster}/nodes/{nodename}/gpu_process_status")
@api_router.get("/{cluster}/nodes/gpu_process_status")
@api_router.get("/{cluster}/nodes/{nodename}/jobs/{job}/gpu_process_status")
async def nodes_sample_process_gpu(
    cluster: str,
    nodename: str | None = None,
    job: int | None = None,
    epoch: int = 0,
    start_time_in_s: float | None = None,
    end_time_in_s: float | None = None,
    resolution_in_s: int | None = None,
    dbi=Depends(db_ops.get_database_v2),
):
    start_time_in_s, end_time_in_s, resolution_in_s = validate_interval(
            start_time_in_s=start_time_in_s,
            end_time_in_s=end_time_in_s,
            resolution_in_s=resolution_in_s
    )

    nodes = None if nodename is None else [nodename]
    return await dbi.get_nodes_sample_process_gpu_timeseries(
            cluster=cluster,
            nodes=nodes,
            job_id=job,
            epoch=epoch,
            start_time_in_s=start_time_in_s,
            end_time_in_s=end_time_in_s,
            resolution_in_s=resolution_in_s
        )


#### Results sorted by jobs

@api_router.get("/{cluster}/jobs/system_process_status")
@api_router.get("/{cluster}/jobs/{job}/system_process_status")
async def job_sample_process_system(
    cluster: str,
    job: int | None = None,
    epoch: int = 0,
    nodename: str | None = None,
    start_time_in_s: float | None = None,
    end_time_in_s: float | None = None,
    resolution_in_s: int | None = None,
    dbi=Depends(db_ops.get_database_v2),
):
    """
    Get combination of cpu process status and gpu process status
    """
    start_time_in_s, end_time_in_s, resolution_in_s = validate_interval(
            start_time_in_s=start_time_in_s,
            end_time_in_s=end_time_in_s,
            resolution_in_s=resolution_in_s
    )

    nodes = None if nodename is None else [nodename]
    return await dbi.get_jobs_sample_process_system_timeseries(
            cluster=cluster,
            nodes=nodes,
            job_id=job,
            epoch=epoch,
            start_time_in_s=start_time_in_s,
            end_time_in_s=end_time_in_s,
            resolution_in_s=resolution_in_s
        )

@api_router.get("/{cluster}/jobs/gpu_process_status")
@api_router.get("/{cluster}/jobs/{job}/gpu_process_status")
async def job_sample_process_gpu(
    cluster: str,
    job: int | None = None,
    epoch: int = 0,
    nodename: str | None = None,
    start_time_in_s: float | None = None,
    end_time_in_s: float | None = None,
    resolution_in_s: int | None = None,
    dbi=Depends(db_ops.get_database_v2),
):
    start_time_in_s, end_time_in_s, resolution_in_s = validate_interval(
            start_time_in_s=start_time_in_s,
            end_time_in_s=end_time_in_s,
            resolution_in_s=resolution_in_s
    )

    nodes = None if nodename is None else [nodename]
    return await dbi.get_jobs_sample_process_gpu_timeseries(
            cluster=cluster,
            nodes=nodes,
            job_id=job,
            epoch=epoch,
            start_time_in_s=start_time_in_s,
            end_time_in_s=end_time_in_s,
            resolution_in_s=resolution_in_s
        )

@api_router.get("/{cluster}/nodes/{nodename}/process_status")
@api_router.get("/{cluster}/nodes/process_status")
async def nodes_process_status(
    cluster: str,
    nodename: str | None = None,
    start_time_in_s: float | None = None,
    end_time_in_s: float | None = None,
    resolution_in_s: int | None = None,
    dbi=Depends(db_ops.get_database_v2),
):
    start_time_in_s, end_time_in_s, resolution_in_s = validate_interval(
            start_time_in_s=start_time_in_s,
            end_time_in_s=end_time_in_s,
            resolution_in_s=resolution_in_s
    )

    nodes = None if nodename is None else [nodename]
    return await dbi.get_nodes_process_status_timeseries(
            cluster=cluster,
            nodes=nodes,
            start_time_in_s=start_time_in_s,
            end_time_in_s=end_time_in_s,
            resolution_in_s=resolution_in_s
        )

@api_router.get("/{cluster}/nodes/{nodename}/cpu_memory_status")
@api_router.get("/{cluster}/nodes/cpu_memory_status")
@api_router.get("/{cluster}/jobs/{job_id}/cpu_memory_status")
@api_router.get("/{cluster}/nodes/{nodename}/jobs/{job_id}/cpu_memory_status")
async def cpu_memory_status(
    cluster: str,
    nodename: str | None = None,
    job_id: int | None = None,
    start_time_in_s: float | None = None,
    end_time_in_s: float | None = None,
    resolution_in_s: int | None = None,
    dbi=Depends(db_ops.get_database_v2),
):
    try:
        start_time_in_s, end_time_in_s, resolution_in_s = validate_interval(
                start_time_in_s=start_time_in_s,
                end_time_in_s=end_time_in_s,
                resolution_in_s=resolution_in_s
        )

        if nodename is None:
            if job_id is None:
                nodes = await dbi.get_nodes(cluster)
            else:
                job = await dbi.get_slurm_job(
                                cluster=cluster,
                                job_id=job_id,
                                start_time_in_s=start_time_in_s,
                                end_time_in_s=end_time_in_s
                            )
                if not job:
                    raise RuntimeError(f"Failed to get information about {job_id=}")
                nodes = job['nodes']
        else:
            nodes = [nodename]

        tasks = {}
        for node in nodes:
            tasks[node] = asyncio.create_task(dbi.get_cpu_status_timeseries(
                    cluster=cluster,
                    node=node,
                    job_id=job_id,
                    start_time_in_s=start_time_in_s,
                    end_time_in_s=end_time_in_s,
                    resolution_in_s=resolution_in_s,
                )
            )
        return { 'cpu_memory_status': {node : (await tasks[node]) for node in nodes}}
    except Exception as e:
        logger.warning(e)
        raise HTTPException(status_code=500,
                detail=str(e))

@api_router.get("/{cluster}/nodes/{nodename}/gpu_status")
@api_router.get("/{cluster}/nodes/gpu_status")
async def gpu_status(
    cluster: str,
    nodename: str | None = None,
    start_time_in_s: float | None = None,
    end_time_in_s: float | None = None,
    resolution_in_s: int | None = None,
    dbi=Depends(db_ops.get_database_v2),
):
    try:
        start_time_in_s, end_time_in_s, resolution_in_s = validate_interval(
                start_time_in_s=start_time_in_s,
                end_time_in_s=end_time_in_s,
                resolution_in_s=resolution_in_s
        )

        nodes = await dbi.get_nodes() if nodename is None else [nodename]
        tasks = {}
        for node in nodes:
            tasks[node] = asyncio.create_task(dbi.get_sample_gpu_timeseries(
                    cluster=cluster,
                    node=nodename,
                    start_time_in_s=start_time_in_s,
                    end_time_in_s=end_time_in_s,
                    resolution_in_s=resolution_in_s,
                )
            )
        return { 'gpu_status': {node : (await tasks[node]) for node in nodes}}
    except Exception as e:
        raise HTTPException(status_code=500,
                detail=str(e))

@api_router.get("/{cluster}/jobs/{job_id}/gpu_status")
@api_router.get("/{cluster}/jobs/{job_id}/{epoch}/gpu_status")
@api_router.get("/{cluster}/nodes/{nodename}/jobs/gpu_status")
@api_router.get("/{cluster}/nodes/{nodename}/jobs/{job_id}/gpu_status")
@api_router.get("/{cluster}/nodes/{nodename}/jobs/{job_id}/{epoch}/gpu_status")
async def job_gpu_status(
    cluster: str,
    job_id: int,
    epoch: int = 0,
    nodename: str | None = None,
    start_time_in_s: float | None = None,
    end_time_in_s: float | None = None,
    resolution_in_s: int | None = None,
    dbi=Depends(db_ops.get_database_v2),
):
    start_time_in_s, end_time_in_s, resolution_in_s = validate_interval(
            start_time_in_s=start_time_in_s,
            end_time_in_s=end_time_in_s,
            resolution_in_s=resolution_in_s
    )

    if nodename is None:
        if job_id is None:
            nodes = await dbi.get_nodes(cluster)
        else:
            job = await dbi.get_job(
                            cluster=cluster,
                            job_id=job_id,
                            epoch=epoch,
                            start_time_in_s=start_time_in_s,
                            end_time_in_s=end_time_in_s
                        )
            if not job:
                raise RuntimeError(f"gpu_status: failed to get information about {job_id=}")
            nodes = job['nodes']
    else:
        nodes = [nodename]

    tasks = {}
    for node in nodes:
        tasks[node] = asyncio.create_task(dbi.get_nodes_sample_process_gpu_timeseries(
                cluster=cluster,
                nodes=[node],
                job_id=job_id,
                epoch=epoch,
                start_time_in_s=start_time_in_s,
                end_time_in_s=end_time_in_s,
                resolution_in_s=resolution_in_s,
            )
        )

    status = {}
    for node in nodes:
        status |= await tasks[node]
    return { 'gpu_status': status}

@api_router.get("/{cluster}/jobs", response_model=None)
@cache(expire=30)
async def jobs(cluster: str,
        start_time_in_s: int | None = None,
        end_time_in_s: int | None = None,
        dbi=Depends(db_ops.get_database_v2),
   ):
    """
    Check current status of jobs
    """
    if end_time_in_s is None:
        end_time_in_s = utcnow().timestamp()

    if start_time_in_s is None:
        # FIXME: WINDOW SIZE
        start_time_in_s = end_time_in_s - 1000000

    return { 'jobs' : await dbi.get_jobs(
            cluster=cluster,
            start_time_in_s=start_time_in_s,
            end_time_in_s=end_time_in_s
        )}

@api_router.get("/{cluster}/job/{job_id}")
@api_router.get("/{cluster}/jobs/{job_id}/info")
async def job_status(
    cluster: str,
    job_id: int,
    start_time_in_s: float | None = None,
    end_time_in_s: float | None = None,
    resolution_in_s: int | None = None,
    dbi=Depends(db_ops.get_database_v2),
):
    return {"job_status": await dbi.get_slurm_job(
            cluster=cluster,job_id=job_id,
            start_time_in_s=start_time_in_s,
            end_time_in_s=end_time_in_s
        )
    }

@api_router.get("/{cluster}/jobs/query")
async def jobs_query(
    cluster: str,
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
    dbi = db_ops.get_database_v2()
    return {"jobs": await dbi.query_jobs(
        cluster=cluster,
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

@api_router.get("/{cluster}/partitions", response_model=None)
@cache(expire=3600)
async def partitions(
        cluster: str,
        time_in_s: int | None = None):
    """
    Check status of partitions
    """
    dbi = db_ops.get_database_v2()
    return await dbi.get_partitions(cluster, time_in_s)


####### v1 ###############################
if False:
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
