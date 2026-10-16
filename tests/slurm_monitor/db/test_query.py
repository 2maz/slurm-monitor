import pytest

from slurm_monitor.db.query import (
    GPUUtilizationAggregationLevel,
    GPUUtilizationBucketLength,
    GPUUtilizationLength,
    QueryMaker,
)

CLUSTER = "cluster-0"
PARAMS = {
    "cluster": CLUSTER,
    "bucket_length": GPUUtilizationBucketLength.FIFTEEN_MINUTES.value,
    "length": GPUUtilizationLength.HOUR.value,
}


def _expected_card_uuids(db_config) -> set[str]:
    return {
        f"gpu-uuid-{CLUSTER}-node-{n}-{gpu}"
        for n in range(db_config.number_of_nodes)
        for gpu in range(db_config.number_of_gpus)
    }


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

    rows = await query.execute_async({**PARAMS, "level": level.value})

    assert rows, f"expected at least one row for level={level.value}"

    # only cards of the requested cluster are included
    expected_cards = _expected_card_uuids(db_config)
    assert {row["gpu_group"] for row in rows} == (expected_values or expected_cards)

    for row in rows:
        assert row.keys() == {
            "period_start",
            "gpu_group",
            "gpu_label",
            "model",
            "manufacturer",
            "gpus_reporting",
            "percent_time_online",
            "avg_utilization_percent_while_online",
        }
        assert row["manufacturer"] == "NVIDIA"
        assert row["model"] == (None if level == GPUUtilizationAggregationLevel.MANUFACTURER else "Tesla V100-SMX3-32GB")
        if level == GPUUtilizationAggregationLevel.CARD:
            assert row["gpus_reporting"] == 1
            # test cards all have index 0, see db_testing
            node = row["gpu_group"].removeprefix("gpu-uuid-").rsplit("-", 1)[0]
            assert row["gpu_label"] == f"{node} / GPU 0"
        else:
            assert 1 <= row["gpus_reporting"] <= len(expected_cards)
        assert row["percent_time_online"] > 0
        assert float(row["avg_utilization_percent_while_online"]) == pytest.approx(
            100.0 / db_config.number_of_jobs, abs=0.01
        )


@pytest.mark.asyncio(loop_scope="module")
async def test_gpu_utilization_level_changes_grouping(test_db, db_config):
    # Regression test: 'level' was once bound as a plain SQL parameter
    # (`:level`) rather than used to select/group by the actual column, so
    # every level silently returned the exact same single group. All test
    # GPUs share one model/manufacturer, so grouping by 'card' must produce
    # many more groups than grouping by 'model' or 'manufacturer'.
    query = QueryMaker().create(db=test_db, name="gpu-utilization")

    card_rows = await query.execute_async({**PARAMS, "level": GPUUtilizationAggregationLevel.CARD.value})
    model_rows = await query.execute_async({**PARAMS, "level": GPUUtilizationAggregationLevel.MODEL.value})

    card_groups = {row["gpu_group"] for row in card_rows}
    model_groups = {row["gpu_group"] for row in model_rows}

    assert len(card_groups) == db_config.number_of_nodes * db_config.number_of_gpus
    assert len(model_groups) == 1
    assert card_groups != model_groups


def test_gpu_utilization_sync_execute(test_db):
    # The CLI (`slurm-monitor query`) goes through the sync `execute`, not
    # `execute_async` - make sure that path also builds a valid statement.
    query = QueryMaker().create(db=test_db, name="gpu-utilization")

    rows = query.execute({**PARAMS, "level": GPUUtilizationAggregationLevel.MANUFACTURER.value})

    assert {row["gpu_group"] for row in rows} == {"NVIDIA"}
