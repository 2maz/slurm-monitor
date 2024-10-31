import pytest
import datetime as dt

from slurm_monitor.db.v1.db_tables import GPUStatus, ProcessStatus, TableBase

def test_gpu_infos(test_db, number_of_nodes, number_of_gpus):
    for i in range(0, number_of_nodes):
        nodename = f"node-{i}"
        gpu_infos = test_db.get_gpu_infos(node=nodename)

        assert "gpus" in gpu_infos
        assert len(gpu_infos["gpus"]) == number_of_gpus

@pytest.mark.asyncio(loop_scope="module")
async def test_gpu_status(test_db, number_of_nodes, number_of_gpus, number_of_samples):
    for i in range(0, number_of_nodes):
        nodename = f"node-{i}"

        resolution_in_s = 10
        gpu_status = await test_db.get_gpu_status(node=nodename, resolution_in_s=resolution_in_s)
        assert len(gpu_status) >= (number_of_samples / resolution_in_s)

@pytest.mark.asyncio(loop_scope="module")
async def test_memory_status(test_db, number_of_nodes, number_of_samples):
    for i in range(0, number_of_nodes):
        nodename = f"node-{i}"

        resolution_in_s = 10

        status = await test_db.get_memory_status(node=nodename, resolution_in_s=resolution_in_s)
        assert len(status) >= (number_of_samples / resolution_in_s)

        start_epoch = status[0].timestamp.replace(tzinfo=dt.timezone.utc).timestamp() - 30
        end_epoch = status[-1].timestamp.replace(tzinfo=dt.timezone.utc).timestamp() + 100

        status = await test_db.get_memory_status(node=nodename,
                start_time_in_s=start_epoch,
                end_time_in_s=end_epoch,
                resolution_in_s=resolution_in_s)

        start_epoch_date = dt.datetime.utcfromtimestamp(start_epoch)
        end_epoch_date = dt.datetime.utcfromtimestamp(end_epoch)
        for x in status:
            assert x.timestamp >= start_epoch_date
            assert x.timestamp <= end_epoch_date
        assert len(status) >= (number_of_samples / resolution_in_s)

@pytest.mark.asyncio(loop_scope="module")
async def test_memory_status_timeseries_list(test_db, number_of_nodes, number_of_samples):
    nodes = [f"node-{i}" for i in range(0, number_of_nodes)]
    resolution_in_s = 10

    status = await test_db.get_memory_status_timeseries_list(
            nodes=nodes,
            resolution_in_s=resolution_in_s)
    assert len(status) == number_of_nodes
    for index, node in enumerate(nodes):
        assert "label" in status[index]
        assert "data" in status[index]

        assert node in status[index]['label']
        assert len(status[index]["data"]) >= (number_of_samples / resolution_in_s)

@pytest.mark.asyncio(loop_scope="module")
async def test_cpu_status_timeseries_list(test_db, number_of_nodes, number_of_samples):
    nodes = [f"node-{i}" for i in range(0, number_of_nodes)]
    resolution_in_s = 10

    status = await test_db.get_cpu_status_timeseries_list(
            nodes=nodes,
            resolution_in_s=resolution_in_s)

    assert len(status) == number_of_nodes
    for index, node in enumerate(nodes):
        assert "label" in status[index]
        assert "data" in status[index]

        assert node in status[index]['label']
        assert len(status[index]["data"]) >= (number_of_samples / resolution_in_s)

@pytest.mark.asyncio(loop_scope="module")
async def test_gpu_status_timeseries_list(test_db, number_of_nodes, number_of_gpus, number_of_samples):
    nodes = [f"node-{i}" for i in range(0, number_of_nodes)]
    resolution_in_s = 10

    status = await test_db.get_gpu_status_timeseries_list(
            nodes=nodes,
            resolution_in_s=resolution_in_s)

    assert len(status) == number_of_nodes*number_of_gpus
    for node_index, node in enumerate(nodes):
        for gpu_index in range(number_of_gpus):
            index = node_index*number_of_gpus + gpu_index
            assert "label" in status[index]
            assert "data" in status[index]

            assert node in status[index]['label']
            assert f"gpu-{gpu_index}" in status[index]['label']
            assert len(status[index]["data"]) >= (number_of_samples / resolution_in_s)

def test_dataframe(test_db, number_of_gpus, number_of_samples):
    uuids = test_db.get_gpu_uuids(node="node-1")
    assert len(uuids) == number_of_gpus

    df = test_db._fetch_dataframe(GPUStatus, GPUStatus.uuid.in_(uuids))
    assert len(df) == number_of_gpus*number_of_samples


def test_apply_resolution_GPUStatus(test_db):
    data = test_db.fetch_all(GPUStatus)
    TableBase.apply_resolution(data, resolution_in_s=100)

def test_apply_resolution_ProcessStatus(test_db):
    data = test_db.fetch_all(ProcessStatus)
    TableBase.apply_resolution(data, resolution_in_s=100)
