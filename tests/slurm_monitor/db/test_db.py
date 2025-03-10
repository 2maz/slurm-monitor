import pytest
import datetime as dt
from pathlib import Path
import re
from zipfile import ZipFile

from slurm_monitor.db.v1.db_tables import (
    GPUStatus,
    JobStatus,
    ProcessStatus,
    TableBase
)
from slurm_monitor.utils import utcnow

@pytest.mark.parametrize("job_status",
    [
        {'start_time': dt.datetime(2024, 11, 13, 11, 36, 2),
         'end_time': dt.datetime(2024, 11, 13, 11, 36, 2),
         'submit_time': dt.datetime(2024, 11, 13, 11, 35, 48),
         'exit_code': 4294967295,
         'job_state': 'CANCELLED',
         'cpus': None,
         'tasks': None,
         'job_id': 352594,
         'name': 'bash',
         'cluster': 'slurm',
         'partition': 'hgx2q',
        }
    ]
)
def test_job_status(job_status, test_db):
    test_db.insert(JobStatus(**job_status))

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

        start_epoch_date = dt.datetime.fromtimestamp(start_epoch, dt.timezone.utc).replace(tzinfo=None)
        end_epoch_date = dt.datetime.fromtimestamp(end_epoch, dt.timezone.utc).replace(tzinfo=None)
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

@pytest.mark.asyncio(loop_scope="module")
async def test_get_last_probe_timestamp(test_db, number_of_nodes):
    timestamps = await test_db.get_last_probe_timestamp()
    assert len(timestamps) == number_of_nodes

@pytest.mark.parametrize("arguments, has_jobs",
        [
            [{}, True],
            [{'min_duration_in_s': lambda: 0}, True],
            [{'max_duration_in_s': lambda: 100000}, True],
            [{'min_duration_in_s': lambda: 100000}, False],
            [{'max_duration_in_s': lambda: 0}, False],
            [{'start_before_in_s': lambda : utcnow().timestamp()}, True],
            [{'start_after_in_s': lambda: utcnow().timestamp()}, False],
            [{'submit_before_in_s': lambda: utcnow().timestamp()}, True],
            [{'submit_after_in_s': lambda: utcnow().timestamp()}, False],
            [{'end_before_in_s': lambda: utcnow().timestamp() + 100}, True],
            [{'end_after_in_s': lambda: utcnow().timestamp() + 100}, False],
        ]
    )
@pytest.mark.asyncio(loop_scope="module")
async def test_get_jobs(arguments, has_jobs, test_db):
    args = {x: y() for x,y in arguments.items()}
    jobs = await test_db.get_jobs(**args)
    assert (len(jobs) > 0) == has_jobs

@pytest.mark.asyncio(loop_scope="module")
async def test_export_data(tmp_path, test_db):
    jobs = test_db.fetch_all(JobStatus)

    prepare_dir = Path(tmp_path) / "prepare"
    prepare_dir.mkdir(parents=True, exist_ok=True)

    for job in jobs[-4:]:
        zip_file = await test_db.export_data(job_id=job.job_id, base_dir=prepare_dir)

        files_in_zip = ZipFile(zip_file).namelist()
        for f in files_in_zip:
            # ensure that only the job related folder will end in the zip
            assert re.match(rf"job-{job.job_id}/.*", f)

        # Ensure that repeated packaging is possible
        zip_file = await test_db.export_data(job_id=job.job_id, base_dir=prepare_dir)
