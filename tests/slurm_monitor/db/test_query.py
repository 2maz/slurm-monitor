import datetime as dt

import pytest
from pydantic import ValidationError

from slurm_monitor.db.db_tables import SampleGpu, SysinfoGpuCard, SysinfoGpuCardConfig
from slurm_monitor.db.query import (
    GPUUtilizationAggregationLevel,
    GPUUtilizationBucketLength,
    GPUUtilizationLength,
    GPUUtilizationParams,
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
            "power_percent_of_limit",
            "avg_power_watts",
            "avg_power_limit_watts",
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
        # test samples all draw 41 W of a 350 W limit, see db_testing
        assert float(row["power_percent_of_limit"]) == pytest.approx(100.0 * 41 / 350, abs=0.1)
        assert row["avg_power_watts"] == 41
        assert row["avg_power_limit_watts"] == 350


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


@pytest.mark.asyncio(loop_scope="function")
async def test_gpu_utilization_merges_uuids_of_rebooted_card(test_db__function_scope):
    # Some cards (e.g. Intel) report node/<boot epoch>/<pci address> as uuid, so
    # one card gets a new uuid on every boot. Here the card reboots halfway
    # through a 15 minute bucket: it must still be one fully online card.
    db = test_db__function_scope
    node = f"{CLUSTER}-node-0"
    uuids = [f"{node}/1759391600/0000:3a:00.0", f"{node}/1761143651/0000:3a:00.0"]
    now = dt.datetime.now(dt.timezone.utc)
    bucket = now.replace(minute=now.minute // 15 * 15, second=0, microsecond=0) - dt.timedelta(minutes=30)
    for i, uuid in enumerate(uuids):
        db.insert(SysinfoGpuCard(uuid=uuid, manufacturer="Intel", model="Max 1100"))
        db.insert(SysinfoGpuCardConfig(cluster=CLUSTER, node=node, uuid=uuid, index=7, time=now))
        # 15 samples every 30s per boot, together covering the whole bucket
        db.insert(
            [
                SampleGpu(uuid=uuid, index=0, ce_util=50, time=bucket + dt.timedelta(seconds=450 * i + 30 * n))
                for n in range(15)
            ]
        )

    query = QueryMaker().create(db=db, name="gpu-utilization")
    card_rows = await query.execute_async({**PARAMS, "level": GPUUtilizationAggregationLevel.CARD.value})
    model_rows = await query.execute_async({**PARAMS, "level": GPUUtilizationAggregationLevel.MODEL.value})
    card_rows = [row for row in card_rows if row["model"] == "Max 1100"]
    model_rows = [row for row in model_rows if row["model"] == "Max 1100"]

    assert {row["gpu_group"] for row in card_rows} == {f"{node}/0000:3a:00.0"}
    assert {row["gpu_label"] for row in card_rows} == {f"{node} / GPU 7"}
    for rows in (card_rows, model_rows):
        [row] = [row for row in rows if row["period_start"] == bucket]
        assert row["gpus_reporting"] == 1
        assert float(row["percent_time_online"]) == pytest.approx(100.0)


@pytest.mark.asyncio(loop_scope="module")
async def test_gpu_utilization_custom_window(test_db):
    # test samples cover the ~12 minutes before the test db was created
    query = QueryMaker().create(db=test_db, name="gpu-utilization")
    now = dt.datetime.now(dt.timezone.utc)
    params = {**PARAMS, "level": GPUUtilizationAggregationLevel.CARD.value}
    del params["length"]

    before = await query.execute_async({**params, "start": now - dt.timedelta(hours=3), "end": now - dt.timedelta(hours=2)})
    assert before == []

    end = now - dt.timedelta(minutes=6)
    rows = await query.execute_async({**params, "start": now - dt.timedelta(hours=1), "end": end})
    assert rows
    assert all(row["period_start"] < end for row in rows)


@pytest.mark.parametrize(
    "window",
    [
        {"length": GPUUtilizationLength.YEAR.value},  # preset with too many buckets
        {"start": "2025-01-01T00:00:00Z", "end": "2026-01-01T00:00:00Z"},  # custom with too many buckets
        {"start": "2026-01-02T00:00:00Z", "end": "2026-01-01T00:00:00Z"},  # end before start
        {"length": GPUUtilizationLength.DAY.value, "start": "2026-01-01T00:00:00Z"},  # both
        {},  # neither
    ],
)
def test_gpu_utilization_params_reject_invalid_windows(window):
    with pytest.raises(ValidationError):
        GPUUtilizationParams(cluster=CLUSTER, level="card", bucket_length="15 minutes", **window)


def test_gpu_utilization_params_accept_custom_window_within_cap():
    GPUUtilizationParams(
        cluster=CLUSTER, level="card", bucket_length="1 day", start="2025-01-01T00:00:00Z", end="2026-01-01T00:00:00Z"
    )
