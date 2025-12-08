# from slurm_monitor.backend.worker import celery_app
import datetime as dt
from fastapi import Depends, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi_cache import FastAPICache
from logging import getLogger, Logger
import pandas as pd
from pathlib import Path
from typing import Annotated

from slurm_monitor.utils import utcnow
from slurm_monitor.app_settings import AppSettings
import slurm_monitor.db_operations as db_ops
from slurm_monitor.api.v2.routes import (
    api_router, cache, get_token_payload, TokenPayload
)

from .response_models import (
    PartitionResponse,
    QueriesResponse,
   JobQueryResultItem,
    JobProfileResultItem,
)

from slurm_monitor.db.v2.query import QueryMaker

logger: Logger = getLogger(__name__)

@api_router.get("/clear_cache")
async def clear_cache():
    await FastAPICache.clear()
    return {"message": "Cache cleared"}

@api_router.get("/cluster/{cluster}/partitions",
        summary="Partitions available in a given cluster",
        tags=["cluster"],
        response_model=list[PartitionResponse]
)
@cache(expire=120)
async def partitions(
        token_payload: Annotated[TokenPayload, Depends(get_token_payload)],
        cluster: str,
        time_in_s: int | None = None):
    """
    Get status of partitions of a cluster (for a specific time point)
    """
    dbi = db_ops.get_database()
    return await dbi.get_partitions(cluster, time_in_s)



#### Results sorted by jobs
#@api_router.get("/cluster/{cluster}/nodes/{nodename}/cpu/timeseries",
#                response_model=dict[str, list[NodeJobSampleProcessTimeseriesResponse]])
#@api_router.get("/cluster/{cluster}/nodes/cpu/timeseries",
#                response_model=dict[str, list[NodeJobSampleProcessTimeseriesResponse]])
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


#@api_router.get("/cluster/{cluster}/nodes/{nodename}/process/cpu/timeseries",
#                response_model=dict[str, CPUMemoryProcessTimeSeriesResponse])
#@api_router.get("/cluster/{cluster}/nodes/process/cpu/timeseries",
#                response_model=dict[str, CPUMemoryProcessTimeSeriesResponse])
#@api_router.get("/cluster/{cluster}/nodes/{nodename}/jobs/{job_id}/process/cpu/timeseries",
#                response_model=dict[str, CPUMemoryProcessTimeSeriesResponse])
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

    user.split(",")
    host.split(",")
    job_id.split(",")

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

@api_router.get("/cluster/{cluster}/queries",
        summary="Get the list of 'named' predefined queries",
        tags=["cluster"],
        response_model=QueriesResponse
)
async def list_queries(
    cluster: str,
):
    return { 'queries': QueryMaker.list_available() }

@api_router.get("/cluster/{cluster}/queries/{query_name}",
    summary="Execute a the 'named' query",
    tags=["cluster"],
    response_model=None
)
async def queries(
    cluster: str,
    query_name: str
):
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

@api_router.get("/cluster/{cluster}/benchmarks/{benchmark_name}")
def benchmarks(cluster: str,
        benchmark_name: str = "lambdal"):
    app_settings = AppSettings.initialize()
    if app_settings.data_dir is None:
        logger.warning("No data directory set. Please set SLURM_MONITOR_DATA_DIR")
        return {}

    path = Path(app_settings.data_dir) / cluster / f"{benchmark_name}-benchmark-results.csv"
    if not path.exists():
        logger.warning(f"{cluster=}: could not find benchmarking results: {path=}")
        return {}

    df = pd.read_csv(str(path))
    df = df.fillna(-1)
    del df['Unnamed: 0']
    return df.to_dict(orient="records")


####### v1 ###############################
if False:
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
