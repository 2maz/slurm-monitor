import asyncio
import base64
import datetime as dt

from fastapi import Depends, HTTPException, Response
from fastapi_cache.decorator import cache
from typing import Annotated

from slurm_monitor.db_operations import DBManager
from slurm_monitor.db.v2.db import ClusterDB
from slurm_monitor.api.v2.routes import (
    api_router,
    validate_interval,
    get_token_payload,
    TokenPayload
)

from slurm_monitor.api.v2.response_models import (
    ErrorMessageResponse,
    NodeInfoResponse,
    NodeStateResponse,
    NodeGpuTimeseriesResponse,
    NodeGpuJobSampleProcessGpuTimeseriesResponse,
    NodeSampleProcessGpuAccResponse,
    SampleProcessAccResponse,
)


@api_router.get("/cluster/{cluster}/nodes",
        summary="Nodes available in a given cluster",
        tags=["cluster"],
        response_model=list[str]
)
@cache(expire=3600)
async def nodes(
        token_payload: Annotated[TokenPayload, Depends(get_token_payload)],
        cluster: str,
        time_in_s: int | None = None,
        dbi: ClusterDB = Depends(DBManager.get_database),
    ):
    """
    Get the list of node names in a cluster
    """
    return await dbi.get_nodes(cluster, time_in_s)

@api_router.get("/cluster/{cluster}/error_messages",
        summary="Error messages collected for the entire cluster",
        tags=["cluster"],
        response_model=dict[str,ErrorMessageResponse]
)
@api_router.get("/cluster/{cluster}/nodes/{nodename}/error_messages",
        summary="Node-specific error messages",
        tags=["node"],
        response_model=dict[str, ErrorMessageResponse]
)
async def error_messages(
        token_payload: Annotated[TokenPayload, Depends(get_token_payload)],
        cluster: str,
        nodename: str | None = None,
        time_in_s: int | None = None,
        dbi: ClusterDB = Depends(DBManager.get_database),
        ):
    """
    Get error_message of a cluster (for a specific time point) (or nodes)
    """
    return await dbi.get_error_messages(cluster, nodename, time_in_s)


@api_router.get("/cluster/{cluster}/nodes/info",
        summary="Detailed information about nodes in a cluster",
        tags=["cluster"],
        response_model=dict[str, NodeInfoResponse]
)
@api_router.get("/cluster/{cluster}/nodes/{nodename}/info",
        summary="Detailed information about a single node in a cluster",
        tags=["node"],
        response_model=dict[str, NodeInfoResponse]
)
@cache(expire=90)
async def nodes_sysinfo(cluster: str,
        token_payload: Annotated[TokenPayload, Depends(get_token_payload)],
        nodename: str | None = None,
        time_in_s: int | None = None,
        dbi: ClusterDB = Depends(DBManager.get_database),
    ):
    """
    Get available information about nodes in a cluster

    It will only contain information about reporting nodes - in some case a
    node might exist in a cluster, but no system information has been received
    yet.  To check - compare with the complete node list /cluster/{cluster}/nodes
    """
    return await dbi.get_nodes_sysinfo(cluster, nodename, time_in_s)

@api_router.get("/cluster/{cluster}/nodes/states",
        summary="Information about the states of all nodes in a cluster",
        tags=["cluster"],
        response_model=list[NodeStateResponse]
)
@api_router.get("/cluster/{cluster}/nodes/{nodename}/states",
        summary="Information about the state(s) of a specific nodes in a cluster",
        tags=["node"],
        response_model=list[NodeStateResponse]
)
async def nodes_states(
        token_payload: Annotated[TokenPayload, Depends(get_token_payload)],
        cluster: str,
        nodename: str | None = None,
        time_in_s: int | None = None,
        dbi: ClusterDB = Depends(DBManager.get_database),
        ):
    """
    Get the state(s) of nodes in a given cluster
    """
    return await dbi.get_nodes_states(cluster, nodename, time_in_s)

@api_router.get("/cluster/{cluster}/nodes/{nodename}/topology",
        summary="Topology information (as image/svg+xml) for a specific node in a cluster",
        tags=["node"],
        response_model=None)
async def nodes_nodename_topology(
        token_payload: Annotated[TokenPayload, Depends(get_token_payload)],
        cluster: str,
        nodename: str,
        dbi: ClusterDB = Depends(DBManager.get_database),
    ):
    """
    Get the topology information for a node (if available)
    """
    node_config = await dbi.get_nodes_sysinfo(cluster, nodename)
    encoded_data = node_config[nodename].get('topo_svg', None)
    if encoded_data:
        data = base64.b64decode(encoded_data).decode('utf-8')
        return Response(content=data, media_type="image/svg+xml")
    else:
        raise HTTPException(status_code=204, # No Content
                detail=f"No topology information available for {nodename=} {cluster=}")


@api_router.get("/cluster/{cluster}/nodes/last_probe_timestamp",
        summary="Get the timestamp of the last message received for each node in the given cluster",
        tags=["cluster"],
        response_model=dict[str, dt.datetime | None])
async def nodes_last_probe_timestamp(
        token_payload: Annotated[TokenPayload, Depends(get_token_payload)],
        cluster: str,
        time_in_s: int = None,
        dbi: ClusterDB = Depends(DBManager.get_database),
        ):
    """
    Retrieve the last known timestamps of records added for nodes in the cluster

    A timestamp of None (or null in the Json response) means, that no monitoring data has been recorded for this node.
    In this case the node has probably no probe, i.e. sonar daemon running.
    """
    return await dbi.get_last_probe_timestamp(cluster=cluster, time_in_s=time_in_s)


@api_router.get("/cluster/{cluster}/nodes/{nodename}/process/gpu/util",
        tags=["node"],
        response_model=NodeSampleProcessGpuAccResponse
)
@api_router.get("/cluster/{cluster}/nodes/process/gpu/util",
        tags=["cluster"],
        response_model=NodeSampleProcessGpuAccResponse
)
async def nodes_process_gpu_util(
    token_payload: Annotated[TokenPayload, Depends(get_token_payload)],
    cluster: str,
    nodename: str | None = None,
    reference_time_in_s: float | None = None,
    window_in_s: int | None = None,
    dbi: ClusterDB = Depends(DBManager.get_database),
):
    """
    Retrieve the latest gpu utilization
    """
    nodes = [nodename] if nodename else (await dbi.get_nodes(cluster))
    tasks = {}
    for node in nodes:
        tasks[node] = asyncio.create_task(dbi.get_node_sample_process_gpu_util(
                cluster=cluster,
                node=node,
                reference_time_in_s=reference_time_in_s,
                window_in_s=window_in_s
            ),
            name=f"nodes_process_gpu_util-{node}"
        )
    return { x: (await task) for x, task in tasks.items()}

@api_router.get("/cluster/{cluster}/nodes/{nodename}/process/gpu/timeseries",
        tags=["node"],
        response_model=NodeGpuJobSampleProcessGpuTimeseriesResponse
)
@api_router.get("/cluster/{cluster}/nodes/process/gpu/timeseries",
        tags=["cluster"],
        response_model=NodeGpuJobSampleProcessGpuTimeseriesResponse
)
@api_router.get("/cluster/{cluster}/nodes/{nodename}/jobs/{job_id}/process/gpu/timeseries",
        summary="Get **node**-related and **job**-related timeseries of GPU samples",
        tags=["node"],
        response_model=NodeGpuJobSampleProcessGpuTimeseriesResponse
)
async def nodes_sample_process_gpu(
    token_payload: Annotated[TokenPayload, Depends(get_token_payload)],
    cluster: str,
    nodename: str | None = None,
    job_id: int | None = None,
    epoch: int = 0,
    start_time_in_s: float | None = None,
    end_time_in_s: float | None = None,
    resolution_in_s: int | None = None,
    dbi: ClusterDB = Depends(DBManager.get_database)
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


@api_router.get("/cluster/{cluster}/nodes/{nodename}/cpu/timeseries",
        summary="Get **node**-specific timeseries data of CPU samples",
        tags=["node"],
        response_model=dict[str, list[SampleProcessAccResponse]]
)
@api_router.get("/cluster/{cluster}/nodes/cpu/timeseries",
        summary="Get timeseries data of CPU samples for all nodes in a given cluster",
        tags=["cluster"],
        response_model=dict[str, list[SampleProcessAccResponse]]
)
@api_router.get("/cluster/{cluster}/nodes/{nodename}/memory/timeseries",
        summary="Get **node**-specific timeseries data of Memory samples",
        tags=["node"],
        response_model=dict[str, list[SampleProcessAccResponse]]
)
@api_router.get("/cluster/{cluster}/nodes/memory/timeseries",
        summary="Get timeseries data of Memory samples for all nodes in a given cluster",
        tags=["cluster"],
        response_model=dict[str, list[SampleProcessAccResponse]]
)
async def nodes_process_cpu_memory_timeseries(
    token_payload: Annotated[TokenPayload, Depends(get_token_payload)],
    cluster: str,
    nodename: str | None = None,
    start_time_in_s: float | None = None,
    end_time_in_s: float | None = None,
    resolution_in_s: int | None = None,
    dbi: ClusterDB = Depends(DBManager.get_database),
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
                ),
                name=f"nodes_process_cpu_memory_timeseries-{node}"
            )
        return { node : (await tasks[node]) for node in nodes}
    except Exception as e:
        raise HTTPException(status_code=500,
                detail=str(e))


@api_router.get("/cluster/{cluster}/nodes/{nodename}/gpu/timeseries",
        summary="Get **node**-specific timeseries data of GPU samples",
        tags=["node"],
        response_model=NodeGpuTimeseriesResponse,
        )
@api_router.get("/cluster/{cluster}/nodes/gpu/timeseries",
        summary="Get timeseries data of GPU samples for all nodes in a given cluster",
        tags=["cluster"],
        response_model=NodeGpuTimeseriesResponse,
        )
async def nodes_sample_gpu(
    token_payload: Annotated[TokenPayload, Depends(get_token_payload)],
    cluster: str,
    nodename: str | None = None,
    start_time_in_s: float | None = None,
    end_time_in_s: float | None = None,
    resolution_in_s: int | None = None,
    dbi: ClusterDB = Depends(DBManager.get_database)
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
                ),
                name=f"nodes_sample_gpu-{node}"
            )
        return { node : (await tasks[node]) for node in nodes}
    except Exception as e:
        raise HTTPException(status_code=500,
                detail=str(e))
