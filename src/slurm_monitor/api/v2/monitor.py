# from slurm_monitor.backend.worker import celery_app
import asyncio
import datetime as dt
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response, FileResponse
from fastapi_cache.decorator import cache
from logging import getLogger, Logger
import pandas as pd
from pathlib import Path

from slurm_monitor.utils import utcnow, fromtimestamp
from slurm_monitor.app_settings import AppSettings
import slurm_monitor.db_operations as db_ops

from .response_models import (
    ClusterResponse,
    ErrorMessageResponse,
    JobResponse,
    PartitionResponse,
    JobsResponse,
    SampleProcessAccResponse,
    NodeStateResponse,
    NodeInfoResponse,
    SystemProcessTimeseriesResponse,
    NodeGpuJobSampleProcessGpuTimeseriesResponse,
    NodeSampleProcessGpuAccResponse,
    JobNodeSampleProcessGpuTimeseriesResponse,
    JobQueryResultItem,
    JobProfileResultItem,
)

from slurm_monitor.db.v2.query import QueryMaker

logger: Logger = getLogger(__name__)

api_router = APIRouter(
#    prefix="",
    tags=["v2"]
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
@api_router.get("/cluster", response_model=list[ClusterResponse])
@cache(expire=3600)
async def cluster(time_in_s: int | None = None):
    """
    Get the list of clusters (available at a particular point in time)
    """
    dbi = db_ops.get_database()
    return await dbi.get_clusters(time_in_s=time_in_s)


@api_router.get("/cluster/{cluster}/partitions", response_model=list[PartitionResponse])
@cache(expire=3600)
async def partitions(
        cluster: str,
        time_in_s: int | None = None):
    """
    Get status of partitions of a cluster (for a specific time point)
    """
    dbi = db_ops.get_database()
    return await dbi.get_partitions(cluster, time_in_s)


@api_router.get("/cluster/{cluster}/nodes", response_model=list[str])
@cache(expire=3600)
async def nodes(
        cluster: str,
        time_in_s: int | None = None
    ):
    """
    Get the list of node names in a cluster
    """

    dbi = db_ops.get_database()
    return await dbi.get_nodes(cluster, time_in_s)

@api_router.get("/cluster/{cluster}/error_messages", response_model=dict[str,ErrorMessageResponse])
@api_router.get("/cluster/{cluster}/nodes/{nodename}/error_messages", response_model=dict[str, ErrorMessageResponse])
async def error_messages(
        cluster: str,
        nodename: str | None = None,
        time_in_s: int | None = None):
    """
    Get error_message of a cluster (for a specific time point) (or nodes)
    """
    dbi = db_ops.get_database()
    return await dbi.get_error_messages(cluster, nodename, time_in_s)


@api_router.get("/cluster/{cluster}/nodes/info", response_model=dict[str, NodeInfoResponse])
@api_router.get("/cluster/{cluster}/nodes/{nodename}/info", response_model=dict[str, NodeInfoResponse])
async def nodes_sysinfo(cluster: str,
        nodename: str | None = None,
        time_in_s: int | None = None
    ):
    """
    Get available information about nodes in a cluster

    It will only contain information about reporting nodes - in some case a node might exist in a cluster, but
    not system information has been received yet. To check - compare with the complete node list /cluster/{cluster}/nodes
    """

    dbi = db_ops.get_database()
    return await dbi.get_nodes_sysinfo(cluster, nodename, time_in_s)

@api_router.get("/cluster/{cluster}/nodes/states", response_model=list[NodeStateResponse])
@api_router.get("/cluster/{cluster}/nodes/{nodename}/states", response_model=list[NodeStateResponse])
async def nodes_states(cluster: str, nodename: str | None = None,
        time_in_s: int | None = None,
        ):
    """
    Get the state(s) of nodes in a given cluster
    """
    dbi = db_ops.get_database()
    return await dbi.get_nodes_states(cluster, nodename, time_in_s)

@api_router.get("/cluster/{cluster}/nodes/{nodename}/topology", response_model=None)
async def nodes_nodename_topology(cluster: str, nodename: str):
    """
    Get the topology information for a node (if available)
    """
    dbi = db_ops.get_database()
    node_config = await dbi.get_nodes_sysinfo(cluster, nodename)
    data = node_config[nodename].get('topo_svg', None)
    if data:
        return Response(content=data, media_type="image/svg+xml")
    else:
        raise HTTPException(status_code=500,
                detail=f"No topology information available for {nodename=} {cluster=}")


@api_router.get("/cluster/{cluster}/nodes/last_probe_timestamp", response_model=dict[str, dt.datetime | None])
async def nodes_last_probe_timestamp(cluster: str):
    """
    Retrieve the last known timestamps of records added for nodes in the cluster

    A timestamp of None (or null in the Json response) means, that no monitoring data has been recorded for this node.
    In this case the node has probably no probe, i.e. sonar daemon running.
    """
    dbi = db_ops.get_database()
    return await dbi.get_last_probe_timestamp(cluster=cluster)


@api_router.get("/cluster/{cluster}/nodes/{nodename}/process/gpu/util", response_model=NodeSampleProcessGpuAccResponse)
@api_router.get("/cluster/{cluster}/nodes/process/gpu/util", response_model=NodeSampleProcessGpuAccResponse)
async def nodes_process_gpu_util(
    cluster: str,
    nodename: str | None = None,
    reference_time_in_s: float | None = None,
    window_in_s: int | None = None,
):
    """
    Retrieve the latest gpu utilization
    """
    dbi = db_ops.get_database()

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

@api_router.get("/cluster/{cluster}/nodes/{nodename}/process/gpu/timeseries", response_model=NodeGpuJobSampleProcessGpuTimeseriesResponse)
@api_router.get("/cluster/{cluster}/nodes/process/gpu/timeseries", response_model=NodeGpuJobSampleProcessGpuTimeseriesResponse)
@api_router.get("/cluster/{cluster}/nodes/{nodename}/jobs/{job_id}/process/gpu/timeseries", response_model=NodeGpuJobSampleProcessGpuTimeseriesResponse)
async def nodes_sample_process_gpu(
    cluster: str,
    nodename: str | None = None,
    job_id: int | None = None,
    epoch: int = 0,
    start_time_in_s: float | None = None,
    end_time_in_s: float | None = None,
    resolution_in_s: int | None = None,
    dbi=Depends(db_ops.get_database),
):
    """
    Get node-related timeseries for processes running on gpu
    """
    start_time_in_s, end_time_in_s, resolution_in_s = validate_interval(
            start_time_in_s=start_time_in_s,
            end_time_in_s=end_time_in_s,
            resolution_in_s=resolution_in_s
    )

    nodes = None if nodename is None else [nodename]
    return await dbi.get_nodes_sample_process_gpu_timeseries(
            cluster=cluster,
            nodes=nodes,
            job_id=job_id,
            epoch=epoch,
            start_time_in_s=start_time_in_s,
            end_time_in_s=end_time_in_s,
            resolution_in_s=resolution_in_s
        )

#### Results sorted by jobs
@api_router.get("/cluster/{cluster}/jobs/process/timeseries", response_model=list[SystemProcessTimeseriesResponse])
@api_router.get("/cluster/{cluster}/jobs/{job_id}/process/timeseries", response_model=list[SystemProcessTimeseriesResponse])
async def job_sample_process_system(
    cluster: str,
    job_id: int | None = None,
    epoch: int = 0,
    nodename: str | None = None,
    start_time_in_s: float | None = None,
    end_time_in_s: float | None = None,
    resolution_in_s: int | None = None,
    dbi=Depends(db_ops.get_database),
):
    """
    Get job-related timeseries for all processes running on cpu and gpu
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

@api_router.get("/cluster/{cluster}/jobs/process/gpu/timeseries", response_model=list[JobNodeSampleProcessGpuTimeseriesResponse])
@api_router.get("/cluster/{cluster}/jobs/{job_id}/process/gpu/timeseries", response_model=list[JobNodeSampleProcessGpuTimeseriesResponse])
async def job_sample_process_gpu_timeseries(
    cluster: str,
    job_id: int | None = None,
    epoch: int = 0,
    nodename: str | None = None,
    start_time_in_s: float | None = None,
    end_time_in_s: float | None = None,
    resolution_in_s: int | None = None,
    dbi=Depends(db_ops.get_database),
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

#@api_router.get("/cluster/{cluster}/nodes/{nodename}/cpu/timeseries", response_model=dict[str, list[NodeJobSampleProcessTimeseriesResponse]])
#@api_router.get("/cluster/{cluster}/nodes/cpu/timeseries", response_model=dict[str, list[NodeJobSampleProcessTimeseriesResponse]])
#async def nodes_process_timeseries(
#    cluster: str,
#    nodename: str | None = None,
#    start_time_in_s: float | None = None,
#    end_time_in_s: float | None = None,
#    resolution_in_s: int | None = None,
#    dbi=Depends(db_ops.get_database),
#):
#    """
#    Get node-related timeseries data for processes running on cpu
#    """
#    start_time_in_s, end_time_in_s, resolution_in_s = validate_interval(
#            start_time_in_s=start_time_in_s,
#            end_time_in_s=end_time_in_s,
#            resolution_in_s=resolution_in_s
#    )
#    nodes = await dbi.get_nodes(cluster=cluster, time_in_s=start_time_in_s) if nodename is None else [nodename]
#    tasks = {}
#    for node in nodes:
#        tasks[node] = asyncio.create_task(dbi.get_node_sample_cpu_timeseries(
#                cluster=cluster,
#                node=node,
#                start_time_in_s=start_time_in_s,
#                end_time_in_s=end_time_in_s,
#                resolution_in_s=resolution_in_s,
#            )
#        )
#    return { node : (await tasks[node]) for node in nodes}
#except Exception as e:
#    raise HTTPException(status_code=500,
#            detail=str(e))

@api_router.get("/cluster/{cluster}/nodes/{nodename}/cpu/timeseries", response_model=dict[str, list[SampleProcessAccResponse]])
@api_router.get("/cluster/{cluster}/nodes/cpu/timeseries", response_model=dict[str, list[SampleProcessAccResponse]])
@api_router.get("/cluster/{cluster}/nodes/{nodename}/memory/timeseries", response_model=dict[str, list[SampleProcessAccResponse]])
@api_router.get("/cluster/{cluster}/nodes/memory/timeseries", response_model=dict[str, list[SampleProcessAccResponse]])
async def nodes_process_cpu_memory_timeseries(
    cluster: str,
    nodename: str | None = None,
    start_time_in_s: float | None = None,
    end_time_in_s: float | None = None,
    resolution_in_s: int | None = None,
    dbi=Depends(db_ops.get_database),
):
    """
    Get node-related timeseries data for processes running on memory
    """
    start_time_in_s, end_time_in_s, resolution_in_s = validate_interval(
            start_time_in_s=start_time_in_s,
            end_time_in_s=end_time_in_s,
            resolution_in_s=resolution_in_s
    )
    try:
        nodes = await dbi.get_nodes(cluster=cluster, time_in_s=start_time_in_s) if nodename is None else [nodename]
        tasks = {}
        for node in nodes:
            tasks[node] = asyncio.create_task(dbi.get_node_sample_process_timeseries(
                    cluster=cluster,
                    node=node,
                    start_time_in_s=start_time_in_s,
                    end_time_in_s=end_time_in_s,
                    resolution_in_s=resolution_in_s,
                )
            )
        return { node : (await tasks[node]) for node in nodes}
    except Exception as e:
        raise HTTPException(status_code=500,
                detail=str(e))

#@api_router.get("/cluster/{cluster}/nodes/{nodename}/process/cpu/timeseries", response_model=dict[str, CPUMemoryProcessTimeSeriesResponse])
##@api_router.get("/cluster/{cluster}/nodes/process/cpu/timeseries", response_model=dict[str, CPUMemoryProcessTimeSeriesResponse])
#@api_router.get("/cluster/{cluster}/nodes/{nodename}/jobs/{job_id}/process/cpu/timeseries", response_model=dict[str, CPUMemoryProcessTimeSeriesResponse])
##@api_router.get("/cluster/{cluster}/jobs/{job_id}/process/cpu/util")
#async def nodes_process_cpu_util(
#    cluster: str,
#    nodename: str | None = None,
#    job_id: int | None = None,
#    start_time_in_s: float | None = None,
#    end_time_in_s: float | None = None,
#    resolution_in_s: int | None = None,
#    dbi=Depends(db_ops.get_database),
#):
#    try:
#        start_time_in_s, end_time_in_s, resolution_in_s = validate_interval(
#                start_time_in_s=start_time_in_s,
#                end_time_in_s=end_time_in_s,
#                resolution_in_s=resolution_in_s
#        )
#
#        if nodename is None:
#            if job_id is None:
#                nodes = await dbi.get_nodes(cluster)
#            else:
#                job = await dbi.get_slurm_job(
#                                cluster=cluster,
#                                job_id=job_id,
#                                start_time_in_s=start_time_in_s,
#                                end_time_in_s=end_time_in_s
#                            )
#                if not job:
#                    raise RuntimeError(f"Failed to get information about {job_id=}. No job with {job_id=}.")
#                nodes = job['nodes']
#        else:
#            nodes = [nodename]
#
#        tasks = {}
#        for node in nodes:
#            tasks[node] = asyncio.create_task(dbi.get_cpu_status_timeseries(
#                    cluster=cluster,
#                    node=node,
#                    job_id=job_id,
#                    start_time_in_s=start_time_in_s,
#                    end_time_in_s=end_time_in_s,
#                    resolution_in_s=resolution_in_s,
#                )
#            )
#        return {node : { 'cpu_memory': (await tasks[node]) } for node in nodes}
#    except Exception as e:
#        logger.warning(e)
#        raise HTTPException(status_code=500,
#                detail=str(e))

@api_router.get("/cluster/{cluster}/nodes/{nodename}/gpu/timeseries")
@api_router.get("/cluster/{cluster}/nodes/gpu/timeseries")
async def nodes_sample_gpu(
    cluster: str,
    nodename: str | None = None,
    start_time_in_s: float | None = None,
    end_time_in_s: float | None = None,
    resolution_in_s: int | None = None,
    dbi=Depends(db_ops.get_database),
):
    try:
        start_time_in_s, end_time_in_s, resolution_in_s = validate_interval(
                start_time_in_s=start_time_in_s,
                end_time_in_s=end_time_in_s,
                resolution_in_s=resolution_in_s
        )

        nodes = await dbi.get_nodes(cluster=cluster, time_in_s=start_time_in_s) if nodename is None else [nodename]
        tasks = {}
        for node in nodes:
            tasks[node] = asyncio.create_task(dbi.get_node_sample_gpu_timeseries(
                    cluster=cluster,
                    node=node,
                    start_time_in_s=start_time_in_s,
                    end_time_in_s=end_time_in_s,
                    resolution_in_s=resolution_in_s,
                )
            )
        return { node : (await tasks[node]) for node in nodes}
    except Exception as e:
        raise HTTPException(status_code=500,
                detail=str(e))


@api_router.get("/cluster/{cluster}/jobs/{job_id}/gpu_status")
@api_router.get("/cluster/{cluster}/jobs/{job_id}/{epoch}/gpu_status")
@api_router.get("/cluster/{cluster}/nodes/{nodename}/jobs/gpu_status")
@api_router.get("/cluster/{cluster}/nodes/{nodename}/jobs/{job_id}/gpu_status")
@api_router.get("/cluster/{cluster}/nodes/{nodename}/jobs/{job_id}/{epoch}/gpu_status")
async def job_gpu_status(
    cluster: str,
    job_id: int,
    epoch: int = 0,
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
                raise RuntimeError(f"gpu_status: failed to get information about {job_id=}"
                        f" {cluster=} {epoch=}"
                        f" {start_time_in_s=} ({fromtimestamp(start_time_in_s)})"
                        f" {end_time_in_s=} ({fromtimestamp(end_time_in_s)})"
                    )
            nodes = job.nodes
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

@api_router.get("/cluster/{cluster}/jobs", response_model=JobsResponse)
@cache(expire=30)
async def jobs(cluster: str,
        start_time_in_s: int | None = None,
        end_time_in_s: int | None = None,
        states: str | None = None,
        dbi=Depends(db_ops.get_database),
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

@api_router.get("/cluster/{cluster}/job/{job_id}", response_model=JobResponse)
@api_router.get("/cluster/{cluster}/jobs/{job_id}/info", response_model=JobResponse)
@api_router.get("/cluster/{cluster}/job/{job_id}/epoch/{epoch}", response_model=JobResponse)
@api_router.get("/cluster/{cluster}/jobs/{job_id}/epoch/{epoch}/info", response_model=JobResponse)
async def job_status(
    cluster: str,
    job_id: int,
    epoch: int = 0,
    start_time_in_s: float | None = None,
    end_time_in_s: float | None = None,
    resolution_in_s: int | None = None,
    states: str | None = None,
    dbi=Depends(db_ops.get_database),
):
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

@api_router.get("/cluster/{cluster}/jobs/query")
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
    states: str = "",
    limit: int = 100,
):
    dbi = db_ops.get_database()
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

## DASHBOARD RELATED
@api_router.get("/jobquery", response_model=list[JobQueryResultItem])
async def dashboard_job_query(
        cluster: str | None = None,
        user: str = '-',
        host: str = '',
        job_id: str = '',
        to: str = '',
        _from: str = Query(default='', alias='from'),
        fmt: str = ''
    ):

    # query job
    # user
    # host
    # duration in s
    #
    # start
    # end
    # cmd

    # Acc

    users = user.split(",")
    hosts = host.split(",")
    jobs = job_id.split(",")

    #dbi = db_ops.get_database()
    #await jobs = dbi.query_jobs(
    #    cluster=cluster,
    #    user_id=None if user == '-' else user,
    #    node=host if host != '' else None,
    #    job_id=int(job_id) if job_id != '' else None,
    #    start_after_in_s=dt.datetime.fromisoformat(_from) if _from != '' else None,
    #    end_after_in_s=dt.datetime.fromisoformat(to) if to != '' else None
    #)


    return [{
            'job': '0',
            'user': "anyuser",
            'host': "anyhost",
            'duration': '100',
            'start': '2025-04-23 08:00:00',
            'end': '2025-04-23 10:00:00',
            'cmd': 'test-command',

            'cpu-peak': 50.0,
            'res-peak': 50.0,
            'mem-peak': 50.0,
            'gpu-peak': 50.0,
            'gpumem-peak': 50.0,
           }]

# FIMXE: parameters are inconsistently named
@api_router.get("/jobprofile", response_model=list[JobProfileResultItem])
async def dashboard_job_profile(
        cluster: str | None = None,
        user: str = '-',
        host: str = '',
        job: int = 0,
        to: str = '',
        _from: str = Query(alias='from'),
        fmt: str = ''
    ):
        end_time = utcnow()
        current_time = end_time - dt.timedelta(hours=12)
        point = {
                     'command': 'test-command',
                     'pid': 5555,
                     'nproc': 1,

                     'cpu': 50.0,
                     'mem': 50.0,
                     'res': 10**5,

                     'gpu': 50.0,
                     'gpumem': 10**5
                }
        samples = []
        while current_time <= end_time:
            samples.append(
                {
                'job': '0',
                'time': str(current_time),
                'points': [point]
               }
            )
            current_time += dt.timedelta(seconds=600)

        return samples

@api_router.get("/cluster/{cluster}/queries")
@api_router.get("/cluster/{cluster}/queries/{query_name}")
async def queries(
    cluster: str,
    query_name: str = None,
):
    if query_name is None:
        return { 'queries': QueryMaker.list_available() }

    try:
        query_maker = QueryMaker(db_ops.get_database())
        query = query_maker.create(query_name, { 'cluster': cluster })
        df = await query.execute_async()

        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(
                status_code=404,
                detail=f"Failed to execute query: '{query_name}' -- {e}"
        )


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
