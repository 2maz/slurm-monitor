import pytest
from slurm_monitor.db.db_tables import GPUs, GPUIdList


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
