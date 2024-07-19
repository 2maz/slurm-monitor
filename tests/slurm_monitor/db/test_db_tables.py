import pytest
from slurm_monitor.db.db_tables import GPUIdList


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
