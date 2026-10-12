import pytest

from slurm_monitor.db.query import (
    GPUUtilizationAggregationLevel,
    GPUUtilizationBucketLength,
    GPUUtilizationLength,
    QueryMaker,
)


def _expected_card_uuids(db_config) -> set[str]:
    nodes = [
        f"{cluster_name}-node-{n}" for cluster_name in db_config.cluster_names for n in range(db_config.number_of_nodes)
    ]
    return {f"gpu-uuid-{node}-{gpu}" for node in nodes for gpu in range(db_config.number_of_gpus)}


@pytest.mark.asyncio(loop_scope="module")
@pytest.mark.parametrize(
    ("level", "expected_values"),
    [
        (GPUUtilizationAggregationLevel.CARD, None),
        (GPUUtilizationAggregationLevel.MODEL, {"Tesla V100-SMX3-32GB"}),
        (GPUUtilizationAggregationLevel.MANUFACTURER, {"NVIDIA"}),
    ],
)
async def test_gpu_utilization_groups_by_level(level, expected_values, test_db, db_config):
    query = QueryMaker().create(db=test_db, name="gpu-utilization")

    rows = await query.execute_async(
        {
            "level": level.value,
            "bucket_length": GPUUtilizationBucketLength.FIFTEEN_MINUTES.value,
            "length": GPUUtilizationLength.HOUR.value,
        },
    )

    assert rows, f"expected at least one row for level={level.value}"

    level_values = {row["level"] for row in rows}
    if expected_values is None:
        expected_values = _expected_card_uuids(db_config)
    assert level_values == expected_values


@pytest.mark.asyncio(loop_scope="module")
async def test_gpu_utilization_level_changes_grouping(test_db, db_config):
    # Regression test: 'level' was once bound as a plain SQL parameter
    # (`:level`) rather than used to select/group by the actual column, so
    # every level silently returned the exact same single group. All test
    # GPUs share one model/manufacturer, so grouping by 'card' must produce
    # many more groups than grouping by 'model' or 'manufacturer'.
    query = QueryMaker().create(db=test_db, name="gpu-utilization")

    params = {
        "bucket_length": GPUUtilizationBucketLength.FIFTEEN_MINUTES.value,
        "length": GPUUtilizationLength.HOUR.value,
    }
    card_rows = await query.execute_async({**params, "level": GPUUtilizationAggregationLevel.CARD.value})
    model_rows = await query.execute_async({**params, "level": GPUUtilizationAggregationLevel.MODEL.value})

    card_groups = {row["level"] for row in card_rows}
    model_groups = {row["level"] for row in model_rows}

    assert len(card_groups) == db_config.number_of_clusters * db_config.number_of_nodes * db_config.number_of_gpus
    assert len(model_groups) == 1
    assert card_groups != model_groups


def test_gpu_utilization_sync_execute(test_db):
    # The CLI (`slurm-monitor query`) goes through the sync `execute`, not
    # `execute_async` - make sure that path also builds a valid statement.
    query = QueryMaker().create(db=test_db, name="gpu-utilization")

    rows = query.execute(
        {
            "level": GPUUtilizationAggregationLevel.MANUFACTURER.value,
            "bucket_length": GPUUtilizationBucketLength.FIFTEEN_MINUTES.value,
            "length": GPUUtilizationLength.HOUR.value,
        },
    )

    assert {row["level"] for row in rows} == {"NVIDIA"}
