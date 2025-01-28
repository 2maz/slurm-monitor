import pytest
from slurm_monitor.db.v1.db_tables import GPUs, GPUIdList, GPUStatus, ProcessStatus
import datetime as dt


@pytest.mark.parametrize(
    "value,indices",
    [
        ["gpu:tesla:1(IDX:3)", [3]],
        ["gpu:a100:5(IDX:1,3-4)", [1, 3, 4]],
        ["gpu:a100:16(IDX:1-3,7,10-12)", [1, 2, 3, 7, 10, 11, 12]],
    ],
)
def test_GPUIdList_get_locical_ids(value, indices):
    assert GPUIdList.get_logical_ids(value) == indices


def test_GPUs():
    args = {
        "uuid": "uuid",
        "node": "node1",
        "model": "Tesla V100",
        "local_id": 0,
        "memory_total": 1E6
    }

    gpu = GPUs(**args)
    assert args == dict(gpu)

def test_GPUStatus_merge():

    now = dt.datetime.now(dt.timezone.utc)
    uuid = "gpu-1"

    status_a = GPUStatus(uuid=uuid,
            temperature_gpu=5,
            power_draw=10,
            utilization_gpu=20,
            utilization_memory=30,
            timestamp=now)

    status_b = GPUStatus(uuid=uuid,
            temperature_gpu=10,
            power_draw=20,
            utilization_gpu=30,
            utilization_memory=40,
            pstate=1,
            timestamp=now)

    status_ab = GPUStatus.merge([status_a, status_b])

    assert status_ab.uuid == uuid
    assert status_ab.temperature_gpu == 7.5
    assert status_ab.power_draw == 15
    assert status_ab.utilization_gpu == 25
    assert status_ab.utilization_memory == 35
    assert status_ab.pstate == 1

def test_ProcessStatus_merge():

    now = dt.datetime.now(dt.timezone.utc)

    status_a = ProcessStatus(
            pid=1,
            job_id=1,
            job_submit_time=now,
            node="node-1",
            cpu_percent=100,
            memory_percent=50,
            timestamp=now
    )

    status_b = ProcessStatus(
            pid=1,
            job_id=1,
            job_submit_time=now,
            node="node-1",
            cpu_percent=50,
            memory_percent=20,
            timestamp=now
    )


    status_ab = ProcessStatus.merge([status_a, status_b])

    assert status_ab.pid == 1
    assert status_ab.job_id == 1
    assert status_ab.job_submit_time == now
    assert status_ab.node == "node-1"
    assert status_ab.timestamp == now

    assert status_ab.cpu_percent == 75
    assert status_ab.memory_percent == 35
