import pytest
from slurm_monitor.db.v2.db_tables import (
    Cluster,
    Partition,
    NodeState,
    SampleProcess,
    SampleProcessGpu,
    SampleSlurmJob,
    SampleSlurmJobAcc,
    SysinfoAttributes,
    SysinfoGpuCard,
    SysinfoGpuCardConfig,
)

from slurm_monitor.db.v2.data_subscriber import DBJsonImporter
from slurm_monitor.utils.command import Command
from slurm_monitor.utils import utcnow
import time
from pathlib import Path
import json

@pytest.fixture
def test_data_dir():
    return Path(__file__).parent.parent.parent.parent.resolve() / "data" / "db" / "v2"

#@pytest.mark.parametrize("sysinfo_filename, ps_filename, cluster, node, gpu_uuids",[
#    ["fox-sysinfo.json", "fox-ps.json", "fox.uio.no", "c1-10.fox", []],
#    ["ml1-sysinfo.json", "ml1-ps.json", "", "ml1.hpc.uio.no", [
#          "GPU-35080357-601c-7113-ec05-f6ca1e58a91e",
#          "GPU-be013a01-364d-ca23-f871-206fe3f259ba",
#          "GPU-daa9f6ac-c8bf-87be-8adc-89b1e7d3f38a",
#        ]]
#    ])
#@pytest.mark.asyncio(loop_scope="module")
#async def test_db_json_import(sysinfo_filename, ps_filename, cluster, node, gpu_uuids,
#        test_data_dir, timescaledb):
#    db_settings = DatabaseSettings(uri=timescaledb)
#    time.sleep(1)
#    db = ClusterDB(db_settings)
#    time.sleep(1)
#    importer = DBJsonImporter(db)
#
#    # Ensure that sysinfo is available before ps can be sampled
#    sysinfo_json = test_data_dir / sysinfo_filename
#    with open(sysinfo_json, 'r') as f:
#        data = json.load(f)
#
#    importer.insert(data)
#
#    ps_json = test_data_dir / ps_filename
#    with open(ps_json, 'r') as f:
#        data = json.load(f)
#
#    importer.insert(data)
#
#    nodes = await db.get_nodes(cluster=cluster)
#    assert nodes == [node]
#
#    gpu_nodes = await db.get_gpu_nodes(cluster=cluster)
#    if gpu_uuids:
#        assert node in gpu_nodes
#    else:
#        assert node not in gpu_nodes
#
#    nodes_info = await db.get_nodes_info(cluster=cluster)

@pytest.mark.asyncio(loop_scope="module")
async def test_get_slurm_jobs(test_db_v2, db_config):
    time_in_s = utcnow().timestamp()
    running_jobs = await test_db_v2.get_slurm_jobs(
            cluster="cluster-0",
            partition="cluster-0-partition-0",
            job_states=["RUNNING"],
            start_time_in_s=time_in_s - 5*60,
            end_time_in_s=time_in_s + 5*60
    )

    assert len(running_jobs) == db_config.number_of_jobs*db_config.number_of_nodes


@pytest.mark.asyncio(loop_scope="module")
async def test_clusters(test_db_v2, db_config):
    clusters = await test_db_v2.get_clusters()
    assert len(clusters) == db_config.number_of_clusters

    for cluster in clusters:
        partitions = await test_db_v2.get_partitions(cluster=cluster['cluster'])
        assert len(partitions) == db_config.number_of_partitions

        # only the first partition has running jobs
        p = partitions[0]
        assert len(p['nodes']) == db_config.number_of_nodes

        job_ids = [x['job_id'] for x in p['jobs_running']]
        assert len(job_ids) == len(set(job_ids))
        assert len(job_ids) == db_config.number_of_jobs*db_config.number_of_nodes

        assert p['total_cpus'] == (2*24*2)*db_config.number_of_nodes
        assert p['gpus_reserved'] == db_config.number_of_jobs*db_config.number_of_nodes

@pytest.mark.asyncio(loop_scope="module")
async def test_get_sample_gpu_timeseries(test_db_v2):

    gpu_timeseries = await test_db_v2.get_sample_gpu_timeseries(
                cluster="cluster-0",
                node='cluster-0-node-1',
                start_time_in_s=utcnow().timestamp() - 3600,
                end_time_in_s=utcnow().timestamp(),
                resolution_in_s=30,
            )
    gpu_data = gpu_timeseries[0]['data']
    assert len(gpu_data) > 0


@pytest.mark.asyncio(loop_scope="module")
async def test_job_sample_process_gpu_timeseries(test_db_v2):

    gpu_timeseries = await test_db_v2.get_jobs_sample_process_gpu_timeseries(
                cluster="cluster-0",
                job_id=1,
                epoch=0,
                start_time_in_s=utcnow().timestamp() - 3600,
                end_time_in_s=utcnow().timestamp(),
                resolution_in_s=30,
                nodes=["cluster-0-node-0"]
            )
    gpu_data = gpu_timeseries[0]['nodes']['cluster-0-node-0']['gpus']
    assert len(gpu_data) == 2
    for gpu, samples in gpu_data.items():
        assert len(samples) > 0



@pytest.mark.asyncio(loop_scope="module")
async def test_nodes(test_db_v2, db_config):
    nodes = await test_db_v2.get_nodes(cluster="cluster-1")
    assert len(nodes) == db_config.number_of_nodes

@pytest.mark.asyncio(loop_scope="module")
async def test_nodes_info(test_db_v2, db_config):

    clusters = await test_db_v2.get_clusters()
    assert len(clusters) == db_config.number_of_clusters

    nodes = await test_db_v2.get_nodes_info(cluster="cluster-1")
    for node, value in nodes.items():
        assert len(value['cards']) == db_config.number_of_gpus




#def test_db_visualize(timescaledb):
#
#    #from sqlalchemy_data_model_visualizer import generate_data_model_diagram
#    #models =[
#    #    Core,
#    #    Node,
#    #    NodeConfig,
#    #    GPUCard,
#    #    GPUCardConfig,
#    #    GPUCardProcessStatus,
#    #    ProcessStatus,
#    #    SlurmJobStatus,
#    #    SoftwareVersion
#    #]
#
#    #generate_data_model_diagram(models, "/tmp/test_output_filename.svg", )
#
#    from sqlalchemy import MetaData
#    from sqlalchemy_schemadisplay import create_schema_graph
#
#    # create the pydot graph object by autoloading all tables via a bound metadata object
#    graph = create_schema_graph(metadata=MetaData(timescaledb),
#       show_datatypes=False, # The image would get nasty big if we'd show the datatypes
#       show_indexes=False, # ditto for indexes
#       rankdir='LR', # From left to right (instead of top to bottom)
#       concentrate=False # Don't try to join the relation lines together
#    )
#    graph.write_png('/tmp/dbschema.png') # write out the file
#
