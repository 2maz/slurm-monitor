import datetime as dt

import pytest
from pydantic import ValidationError

from slurm_monitor.db.db_tables import (
    Node,
    NodeState,
    SampleGpu,
    SampleProcessGpu,
    SampleSlurmJob,
    SampleSystem,
    SysinfoAttributes,
    SysinfoGpuCard,
    SysinfoGpuCardConfig,
)
from slurm_monitor.db.query import (
    CPUUtilizationAggregationLevel,
    CPUUtilizationParams,
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
        assert row["model"] == (
            None if level == GPUUtilizationAggregationLevel.MANUFACTURER else "Tesla V100-SMX3-32GB"
        )
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
    params = {
        "cluster": CLUSTER,
        "bucket_length": PARAMS["bucket_length"],
        "level": GPUUtilizationAggregationLevel.CARD.value,
    }

    before = await query.execute_async(
        {**params, "start": now - dt.timedelta(hours=3), "end": now - dt.timedelta(hours=2)}
    )
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


@pytest.mark.asyncio(loop_scope="function")
@pytest.mark.parametrize("level", list(CPUUtilizationAggregationLevel))
async def test_cpu_utilization_follows_counters_across_reboot(level, test_db__function_scope):
    # cpus holds busy seconds since boot per logical CPU. The node is half busy for the bucket
    # before, and in the checked bucket it is half busy for 450s, reboots, then fully busy for 420s.
    db = test_db__function_scope
    node = f"{CLUSTER}-node-0"
    now = dt.datetime.now(dt.timezone.utc)
    bucket = now.replace(minute=now.minute // 15 * 15, second=0, microsecond=0) - dt.timedelta(minutes=30)
    old_boot, new_boot = bucket - dt.timedelta(days=3), bucket + dt.timedelta(seconds=440)
    # test nodes have 2 sockets x 24 cores x 2 threads and 256 GiB, see db_testing
    cpus, memory = 96, 256 * 1024**2

    def sample(n: int, boot: dt.datetime, busy: int) -> SampleSystem:
        return SampleSystem(
            cluster=CLUSTER,
            node=node,
            boot=boot,
            cpus=[busy] * cpus,
            used_memory=memory // 4,
            load1=0,
            load5=0,
            load15=0,
            runnable_entities=0,
            existing_entities=0,
            time=bucket + dt.timedelta(seconds=30 * n),
        )

    db.insert(
        [sample(n, old_boot, 100000 + 15 * n) for n in range(-30, 15)]
        + [sample(n, new_boot, 30 * (n - 15)) for n in range(15, 30)]
    )

    query = QueryMaker().create(db=db, name="cpu-utilization")
    rows = await query.execute_async({**PARAMS, "level": level.value})

    group = node if level == CPUUtilizationAggregationLevel.NODE else "Intel Xeon"
    assert {row["cpu_group"] for row in rows} == {group}
    before, during = sorted(rows, key=lambda row: row["period_start"])
    assert before["period_start"] == bucket - dt.timedelta(minutes=15)
    assert during["period_start"] == bucket
    for row in (before, during):
        assert row.keys() == {
            "period_start",
            "cpu_group",
            "model",
            "nodes_reporting",
            "logical_cpus",
            "percent_time_online",
            "avg_utilization_percent_while_online",
            "memory_percent_used",
        }
        assert row["model"] == "Intel Xeon"
        assert row["nodes_reporting"] == 1
        assert row["logical_cpus"] == cpus
        assert float(row["percent_time_online"]) == pytest.approx(100.0)
        assert float(row["memory_percent_used"]) == pytest.approx(25.0)
    assert float(before["avg_utilization_percent_while_online"]) == pytest.approx(50.0)
    # continues from the bucket before up to the reboot, then starts over from the first new sample
    assert float(during["avg_utilization_percent_while_online"]) == pytest.approx(
        100.0 * (15 * 15 + 30 * 14) / (450 + 420), abs=0.01
    )


def test_cpu_utilization_params_reject_gpu_levels():
    with pytest.raises(ValidationError):
        CPUUtilizationParams(cluster=CLUSTER, level="card", bucket_length="15 minutes", length="1 hour")


@pytest.mark.asyncio(loop_scope="function")
async def test_gpu_allocation_layers(test_db__function_scope):
    # In the checked 15 minute bucket node-a has 2 cards, both allocated to job 1 and online throughout.
    # Card 0 runs job 1 at 80%, card 1 runs a process outside Slurm for half the bucket.
    # Job 2 has 4 GPUs over node-b and node-c, unmonitored, for the second half. node-a drains for 5 minutes.
    # node-d's card is drained throughout and node-e's never allocated, so neither is Slurm's capacity.
    db = test_db__function_scope
    cluster = "cluster-gpu-allocation"
    node_a, node_b, node_c, node_d, node_e, node_f, node_g, node_h, node_i = (
        f"{cluster}-node-{x}" for x in "abcdefghi"
    )
    now = dt.datetime.now(dt.timezone.utc)
    bucket = now.replace(minute=now.minute // 15 * 15, second=0, microsecond=0) - dt.timedelta(minutes=30)
    at = lambda seconds: bucket + dt.timedelta(seconds=seconds)  # noqa: E731

    db.insert(
        [
            Node(cluster=cluster, node=node)
            for node in (node_a, node_b, node_c, node_d, node_e, node_f, node_g, node_h, node_i)
        ]
    )
    # node-h and node-i last reported their cards 10 days ago; node-h still runs a GPU job
    for node in (node_h, node_i):
        uuid = f"gpu-uuid-{node}-0"
        db.insert(SysinfoGpuCard(uuid=uuid, manufacturer="AMD", model="Instinct MI210"))
        db.insert(SysinfoGpuCardConfig(cluster=cluster, node=node, uuid=uuid, index=0, time=at(-10 * 86400)))
    # node-g's 2 cards, as on ex3's n016, under 4 uuids: AMD ids that changed, and node/<boot epoch>/<address>
    # ones, with the address's PCI domain in 4 or 8 digits
    for uuid, address, seconds in (
        ("67c63ac3c3c637f9", "0000:03:00.0", 0),
        ("7b6b270993774982", "00000000:03:00.0", 300),
        ("95a1ca7691e7c391", "0000:83:00.0", 0),
        (f"{node_g}/1791632042/00000000:83:00.0", "00000000:83:00.0", 300),
    ):
        db.insert(SysinfoGpuCard(uuid=uuid, manufacturer="AMD", model="Instinct MI210"))
        db.insert(
            SysinfoGpuCardConfig(cluster=cluster, node=node_g, uuid=uuid, address=address, index=0, time=at(seconds))
        )
    # node-f is added halfway through the bucket, with 2 cards
    for index in range(2):
        uuid = f"gpu-uuid-{node_f}-{index}"
        db.insert(SysinfoGpuCard(uuid=uuid, manufacturer="NVIDIA", model="H100"))
        db.insert(SysinfoGpuCardConfig(cluster=cluster, node=node_f, uuid=uuid, index=index, time=at(450)))
    cards = [f"gpu-uuid-{node_a}-{gpu}" for gpu in range(2)]
    for index, uuid in enumerate(cards):
        db.insert(SysinfoGpuCard(uuid=uuid, manufacturer="NVIDIA", model="A100"))
        db.insert(SysinfoGpuCardConfig(cluster=cluster, node=node_a, uuid=uuid, index=index, time=at(0)))
        db.insert([SampleGpu(uuid=uuid, index=index, ce_util=80 * (1 - index), time=at(30 * n)) for n in range(30)])
    for node in (node_d, node_e):
        uuid = f"gpu-uuid-{node}-0"
        db.insert(SysinfoGpuCard(uuid=uuid, manufacturer="NVIDIA", model="A100"))
        db.insert(SysinfoGpuCardConfig(cluster=cluster, node=node, uuid=uuid, index=0, time=at(0)))
    # node-e's card also reported under a new uuid after a reboot, seen in samples before its sysinfo
    rebooted = f"{node_e}/1761143651/0000:3a:00.0"
    db.insert(SysinfoGpuCard(uuid=rebooted, manufacturer="", model=""))
    db.insert(SysinfoGpuCardConfig(cluster=cluster, node=node_e, uuid=rebooted, index=1, time=at(0)))
    db.insert(SysinfoGpuCard(uuid=f"{node_e}/1759391600/0000:3a:00.0", manufacturer="Intel", model="Max 1100"))
    db.insert(
        SysinfoGpuCardConfig(
            cluster=cluster, node=node_e, uuid=f"{node_e}/1759391600/0000:3a:00.0", index=1, time=at(0)
        )
    )

    def process(uuid: str, job: int, epoch: int, n: int) -> SampleProcessGpu:
        return SampleProcessGpu(
            cluster=cluster, node=node_a, job=job, epoch=epoch, pid=job, uuid=uuid, gpu_util=0, time=at(30 * n)
        )

    db.insert(
        [process(cards[0], 1, 0, n) for n in range(30)]
        # two processes in a sample still occupy one card
        + [process(cards[1], pid, 1759391600, n) for n in range(15) for pid in (4242, 4343)]
    )

    def job(job_id: int, step: str, nodes: list[str], resources: str, started: int, n: int) -> SampleSlurmJob:
        return SampleSlurmJob(
            cluster=cluster,
            job_id=job_id,
            job_step=step,
            job_name=f"job-{job_id}",
            job_state="RUNNING",
            user_name="any-user",
            account="any-account",
            start_time=at(started),
            suspend_time=0,
            submit_time=at(started - 60),
            time_limit=3600,
            partition="any-partition",
            reservation="",
            nodes=nodes,
            priority=1,
            distribution="",
            requested_cpus=8,
            requested_memory_per_node=0,
            requested_node_count=len(nodes),
            requested_resources=resources,
            allocated_resources=resources,
            time=at(started + 60 * n),
        )

    # sampled every 60 s, up to a minute before the bucket ends
    db.insert(
        [job(1, "", [node_a], "billing=8,cpu=8,gres/gpu=2,node=1", 0, n) for n in range(15)]
        # steps repeat the job's allocation and must not count again
        + [job(1, "batch", [node_a], "cpu=8,gres/gpu=2,mem=0,node=1", 0, n) for n in range(15)]
        + [job(2, "", [node_b, node_c], "billing=16,cpu=16,gres/gpu=4,node=2", 450, n) for n in range(8)]
        + [job(3, "", [node_a], "billing=8,cpu=8,node=1", 0, n) for n in range(15)]
        + [job(4, "", [node_h], "billing=8,cpu=8,gres/gpu=1,node=1", 0, n) for n in range(15)]
    )
    db.insert(
        [
            NodeState(cluster=cluster, node=node_a, states=["ALLOCATED"], time=at(-600)),
            NodeState(cluster=cluster, node=node_a, states=["ALLOCATED", "DRAIN"], time=at(300)),
            NodeState(cluster=cluster, node=node_a, states=["ALLOCATED"], time=at(600)),
            NodeState(cluster=cluster, node=node_d, states=["IDLE", "DRAIN"], time=now - dt.timedelta(minutes=90)),
        ]
    )

    query = QueryMaker().create(db=db, name="gpu-allocation")
    rows = await query.execute_async({**PARAMS, "cluster": cluster})

    # node-i has no cards in the window and no GPU jobs, so nothing to show
    assert {row["node"] for row in rows} == {node_a, node_b, node_c, node_d, node_e, node_f, node_g, node_h}
    before = {row["node"]: row for row in rows if row["period_start"] == bucket - dt.timedelta(minutes=15)}
    rows = {row["node"]: row for row in rows if row["period_start"] == bucket}
    for row in rows.values():
        assert row.keys() == {
            "period_start",
            "node",
            "model",
            "manufacturer",
            "gpus_installed",
            "gpus_online",
            "gpus_allocated",
            "gpus_occupied",
            "gpus_occupied_outside_slurm",
            "gpus_busy",
            "percent_time_unavailable",
            "gpu_jobs",
            "not_counted",
        }

    a = rows[node_a]
    assert a["not_counted"] is None
    assert (a["model"], a["manufacturer"]) == ("A100", "NVIDIA")
    assert a["gpus_installed"] == 2
    assert a["gpu_jobs"] == 1
    assert float(a["gpus_online"]) == pytest.approx(2.0)
    assert float(a["gpus_allocated"]) == pytest.approx(2.0)
    assert float(a["gpus_occupied"]) == pytest.approx(1.0)
    assert float(a["gpus_occupied_outside_slurm"]) == pytest.approx(0.5)
    assert float(a["gpus_busy"]) == pytest.approx(0.8)
    assert float(a["percent_time_unavailable"]) == pytest.approx(100 / 3, abs=0.1)

    for node in (node_b, node_c):
        row = rows[node]
        assert row["not_counted"] == "not monitored"
        assert row["model"] is None
        assert row["gpus_installed"] == 0
        assert row["gpu_jobs"] == 1
        assert float(row["gpus_online"]) == 0
        # half of 4 GPUs each, for half the bucket
        assert float(row["gpus_allocated"]) == pytest.approx(1.0)
        assert float(row["percent_time_unavailable"]) == 0

    assert rows[node_d]["not_counted"] == "out of service"
    assert float(rows[node_d]["percent_time_unavailable"]) == pytest.approx(100.0)
    assert rows[node_e]["not_counted"] == "not a Slurm GPU node"
    # the rebooted card counts once, named after the uuid that has its sysinfo
    assert rows[node_e]["gpus_installed"] == 2
    assert (rows[node_e]["model"], rows[node_e]["manufacturer"]) == ("A100 + Max 1100", "Intel + NVIDIA")
    # cards reported only before the window don't make node-h monitored
    assert rows[node_h]["not_counted"] == "not monitored"
    assert float(rows[node_h]["gpus_installed"]) == 0
    assert float(rows[node_h]["gpus_allocated"]) == pytest.approx(1.0)
    # cards are told apart by their node and address, whatever their uuids
    assert float(rows[node_g]["gpus_installed"]) == pytest.approx(2.0)
    # cards count only once they are there
    assert float(before[node_f]["gpus_installed"]) == 0
    assert float(rows[node_f]["gpus_installed"]) == pytest.approx(1.0)
    assert float(before[node_a]["gpus_installed"]) == 0


def test_gpu_allocation_sync_execute(test_db):
    query = QueryMaker().create(db=test_db, name="gpu-allocation")

    rows = query.execute(PARAMS)

    assert rows
    assert {row["node"] for row in rows} == {f"{CLUSTER}-node-{n}" for n in range(2)}
    assert {row["not_counted"] for row in rows} == {None}


@pytest.mark.asyncio(loop_scope="function")
async def test_cpu_allocation_layers(test_db__function_scope):
    # In the checked 15 minute bucket node-a's 96 logical CPUs are half busy. Job 10 has 48 of them throughout,
    # job 11 has 32 CPUs over node-a and node-b, unmonitored, for the second half. node-c is drained throughout.
    db = test_db__function_scope
    cluster = "cluster-cpu-allocation"
    node_a, node_b, node_c, node_d, node_e, node_f = (f"{cluster}-node-{x}" for x in "abcdef")
    now = dt.datetime.now(dt.timezone.utc)
    bucket = now.replace(minute=now.minute // 15 * 15, second=0, microsecond=0) - dt.timedelta(minutes=30)
    at = lambda seconds: bucket + dt.timedelta(seconds=seconds)  # noqa: E731
    cpus = 96

    db.insert([Node(cluster=cluster, node=node) for node in (node_a, node_b, node_c, node_d, node_e, node_f)])
    # node-d is added halfway through the bucket; node-e reported 10 days ago, then stopped until halfway
    # through it, which is a gap in its reports rather than it being added
    # node-f last reported 10 days ago, but still runs a job
    for node, since in (
        (node_a, 0),
        (node_c, 0),
        (node_d, 450),
        (node_e, -10 * 86400),
        (node_e, 450),
        (node_f, -10 * 86400),
    ):
        db.insert(
            SysinfoAttributes(
                cluster=cluster,
                node=node,
                sockets=2,
                cores_per_socket=24,
                threads_per_core=2,
                cpu_model="Intel Xeon",
                memory=256 * 1024**2,
                time=at(since),
            )
        )
    # from the bucket before, so the checked bucket's busy time runs over all of it
    db.insert(
        [
            SampleSystem(
                cluster=cluster,
                node=node_a,
                boot=bucket - dt.timedelta(days=3),
                cpus=[100000 + 15 * n] * cpus,
                used_memory=0,
                load1=0,
                load5=0,
                load15=0,
                runnable_entities=0,
                existing_entities=0,
                time=at(30 * n),
            )
            for n in range(-30, 30)
        ]
    )

    def job(job_id: int, nodes: list[str], resources: str, started: int, n: int) -> SampleSlurmJob:
        return SampleSlurmJob(
            cluster=cluster,
            job_id=job_id,
            job_step="",
            job_name=f"job-{job_id}",
            job_state="RUNNING",
            user_name="any-user",
            account="any-account",
            start_time=at(started),
            suspend_time=0,
            submit_time=at(started - 60),
            time_limit=3600,
            partition="any-partition",
            reservation="",
            nodes=nodes,
            priority=1,
            distribution="",
            requested_resources=resources,
            allocated_resources=resources,
            time=at(started + 60 * n),
        )

    db.insert(
        [job(10, [node_a], "billing=48,cpu=48,node=1", 0, n) for n in range(15)]
        + [job(11, [node_a, node_b], "billing=32,cpu=32,node=2", 450, n) for n in range(8)]
        + [job(12, [node_f], "billing=8,cpu=8,node=1", 0, n) for n in range(15)]
    )
    db.insert(NodeState(cluster=cluster, node=node_c, states=["IDLE", "DRAIN"], time=now - dt.timedelta(minutes=90)))

    query = QueryMaker().create(db=db, name="cpu-allocation")
    rows = await query.execute_async({**PARAMS, "cluster": cluster})

    assert {row["node"] for row in rows} == {node_a, node_b, node_c, node_d, node_e, node_f}
    rows = {row["node"]: row for row in rows if row["period_start"] == bucket}
    assert float(rows[node_d]["cpus_installed"]) == pytest.approx(cpus / 2)
    assert float(rows[node_e]["cpus_installed"]) == pytest.approx(cpus)
    assert rows[node_f]["not_counted"] == "not monitored"
    for row in rows.values():
        assert row.keys() == {
            "period_start",
            "node",
            "model",
            "cpus_installed",
            "cpus_online",
            "cpus_allocated",
            "cpus_busy",
            "percent_time_unavailable",
            "jobs",
            "not_counted",
        }

    a = rows[node_a]
    assert a["not_counted"] is None
    assert a["cpus_installed"] == cpus
    assert a["model"] == "Intel Xeon"
    assert a["jobs"] == 2
    assert float(a["cpus_online"]) == pytest.approx(cpus)
    assert float(a["cpus_allocated"]) == pytest.approx(48 + 16 / 2)
    assert float(a["cpus_busy"]) == pytest.approx(cpus / 2)
    assert float(a["percent_time_unavailable"]) == 0

    b = rows[node_b]
    assert b["not_counted"] == "not monitored"
    assert b["cpus_installed"] == 0
    assert float(b["cpus_allocated"]) == pytest.approx(16 / 2)

    c = rows[node_c]
    assert c["not_counted"] == "out of service"
    assert c["cpus_installed"] == cpus
    assert c["jobs"] == 0
    assert float(c["percent_time_unavailable"]) == pytest.approx(100.0)
