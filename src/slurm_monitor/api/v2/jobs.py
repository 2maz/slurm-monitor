
from fastapi import Depends
from fastapi_cache.decorator import cache
from typing import Annotated


from slurm_monitor.utils import utcnow
from slurm_monitor.db_operations import DBManager
from slurm_monitor.db.v2.db import ClusterDB
from slurm_monitor.api.v2.routes import (
    api_router,
    validate_interval,
    get_token_payload,
    TokenPayload
)

from slurm_monitor.api.v2.response_models import (
    JobResponse,
    JobsResponse,
    SystemProcessTimeseriesResponse,
    JobNodeSampleProcessGpuTimeseriesResponse,
)

@api_router.get("/cluster/{cluster}/jobs/process/timeseries",
        summary="Get all jobs process timeseries-data (cpu/memory/gpu) on a given cluster",
        tags=["cluster"],
        response_model=list[SystemProcessTimeseriesResponse]
)
@api_router.get("/cluster/{cluster}/jobs/{job_id}/process/timeseries",
        summary="Get **job**-specific process (cpu/memory/gpu) timeseries data",
        tags=["job"],
        response_model=list[SystemProcessTimeseriesResponse]
)
@cache(expire=90)
async def job_sample_process_system(
    token_payload: Annotated[TokenPayload, Depends(get_token_payload)],
    cluster: str,
    job_id: int | None = None,
    epoch: int = 0,
    nodename: str | None = None,
    start_time_in_s: float | None = None,
    end_time_in_s: float | None = None,
    resolution_in_s: int | None = None,
    dbi: ClusterDB = Depends(DBManager.get_database)
):
    """
    Get job-related timeseries for all processes running on cpu and gpu

    By default this related to SLURM jobs (epoch set to 0).
    To relate to non-SLURM jobs, provide the epoch as parameter to the query.

    That will be separated in 'cpu_memory' and 'gpus'
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
            job_id=job_id,
            epoch=epoch,
            start_time_in_s=start_time_in_s,
            end_time_in_s=end_time_in_s,
            resolution_in_s=resolution_in_s
        )

@api_router.get("/cluster/{cluster}/jobs/process/gpu/timeseries",
        summary="Get GPU samples as timeseries, aggregated per job (for all jobs) and process on a given cluster",
        tags=["cluster"],
        response_model=list[JobNodeSampleProcessGpuTimeseriesResponse]
)
@api_router.get("/cluster/{cluster}/jobs/{job_id}/process/gpu/timeseries",
        summary="Get GPU sample as timeseries aggregate for a specific job on a given cluster",
        tags=["job"],
        response_model=list[JobNodeSampleProcessGpuTimeseriesResponse]
)
async def job_sample_process_gpu_timeseries(
    token_payload: Annotated[TokenPayload, Depends(get_token_payload)],
    cluster: str,
    job_id: int | None = None,
    epoch: int = 0,
    nodename: str | None = None,
    start_time_in_s: float | None = None,
    end_time_in_s: float | None = None,
    resolution_in_s: int | None = None,
    dbi: ClusterDB = Depends(DBManager.get_database)
):
    """
    Get job-related timeseries data for processes running on gpu
    """
    start_time_in_s, end_time_in_s, resolution_in_s = validate_interval(
            start_time_in_s=start_time_in_s,
            end_time_in_s=end_time_in_s,
            resolution_in_s=resolution_in_s
    )

    nodes = None if nodename is None else [nodename]
    data = await dbi.get_jobs_sample_process_gpu_timeseries(
            cluster=cluster,
            nodes=nodes,
            job_id=job_id,
            epoch=epoch,
            start_time_in_s=start_time_in_s,
            end_time_in_s=end_time_in_s,
            resolution_in_s=resolution_in_s
        )
    return data

@api_router.get("/cluster/{cluster}/jobs",
        summary="Get jobs running on the given cluster",
        tags=["cluster"],
        response_model=JobsResponse)
@cache(expire=30)
async def jobs(cluster: str,
    token_payload: Annotated[TokenPayload, Depends(get_token_payload)],
    start_time_in_s: int | None = None,
    end_time_in_s: int | None = None,
    states: str | None = None,
    dbi: ClusterDB = Depends(DBManager.get_database)
   ):
    """
    Check current status of jobs
    """
    if end_time_in_s is None:
        end_time_in_s = utcnow().timestamp()

    if start_time_in_s is None:
        start_time_in_s = end_time_in_s - 60*15 # last 15 min

    job_states = None
    if states:
        job_states = states.split(",")

    return { 'jobs' : await dbi.get_jobs(
                cluster=cluster,
                start_time_in_s=start_time_in_s,
                end_time_in_s=end_time_in_s,
                states=job_states
        )}

@api_router.get("/cluster/{cluster}/jobs/{job_id}",
        summary="Get SLURM job information by id for the given cluster",
        tags=["job"],
        response_model=JobResponse
)
@api_router.get("/cluster/{cluster}/jobs/{job_id}/info",
        summary="Get SLURM job information by id for the given cluster",
        tags=["job"],
        response_model=JobResponse
)
@api_router.get("/cluster/{cluster}/jobs/{job_id}/epoch/{epoch}",
        summary="Get job information by id and epoch for the given cluster",
        tags=["job"],
        response_model=JobResponse)
@api_router.get("/cluster/{cluster}/jobs/{job_id}/epoch/{epoch}/info",
        summary="Get job information by id and epoch for the given cluster",
        tags=["job"],
        response_model=JobResponse)
async def job_status(
    token_payload: Annotated[TokenPayload, Depends(get_token_payload)],
    cluster: str,
    job_id: int,
    epoch: int = 0,
    start_time_in_s: float | None = None,
    end_time_in_s: float | None = None,
    resolution_in_s: int | None = None,
    states: str | None = None,
    dbi: ClusterDB = Depends(DBManager.get_database)
):
    """
    Get job information optionally limited by a given timeframe and output provided in a specified resolution of time
    """
    job_states = None
    if states:
        job_states = states.split(",")

    return await dbi.get_job(
                cluster=cluster,
                job_id=job_id,
                epoch=epoch,
                start_time_in_s=start_time_in_s,
                end_time_in_s=end_time_in_s,
                states=job_states
    )

@api_router.get("/cluster/{cluster}/query/jobs",
        summary="Provides a generic job query interface",
        tags=["cluster"],
        response_model=None
        )
async def query_jobs(
    token_payload: Annotated[TokenPayload, Depends(get_token_payload)],
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
    states: str = "",
    limit: int = 100,
    dbi: ClusterDB = Depends(DBManager.get_database)
):
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
        states=states.split(","),
        limit=limit
        )
    }
