import pytest
import asyncio
import datetime as dt
from pathlib import Path
import json
import sqlalchemy
import time

from kafka import TopicPartition

from slurm_monitor.utils import utcnow
from slurm_monitor.db.v2 import message_subscriber as message_subscriber_module
from slurm_monitor.db.v2.message_subscriber import MessageSubscriber
from slurm_monitor.db.v2.importer import DBJsonImporter


class MockKafkaMessageRecord:
    topic: str
    offset: int
    value: bytes
    timestamp: int

    def __init__(self, topic: str, message_data: str, offset: int = 0, timestamp: int | None = None):
        self.topic = topic
        self.value = message_data.encode("UTF-8")
        self.offset = offset
        self.timestamp = timestamp if timestamp is not None else int(utcnow().timestamp()*1000)


class MockKafkaConsumer:
    """
    Stands in for a real `kafka.KafkaConsumer` bound to a single topic's
    worth of sonar messages (loaded from `message_files`), driven via
    `poll()` the same way `consume_topic()` drives a real one.
    """
    records: list[MockKafkaMessageRecord]

    cluster: str | None
    topic_offsets: dict[str, int]

    def __init__(self, message_files: list[str], test_data_dir: Path):
        self.cluster = None
        self.topic_offsets = {}
        self.records = []

        for sonar_msg_file in message_files:
            json_filename = Path(test_data_dir) / "sonar" / sonar_msg_file
            with open(json_filename, "r") as f:
                data = json.load(f)

                timestamp = (utcnow() - dt.timedelta(hours=1))
                cluster = data['data']['attributes']['cluster']
                data['data']['attributes']['time'] = timestamp.isoformat()

                if not self.cluster:
                    self.cluster = cluster
                elif self.cluster != cluster:
                    raise RuntimeError(f"Cluster expected message for one cluster, but got {self.cluster} and {cluster}")

                topic = cluster + "." + data['data']['type']
                if topic not in self.topic_offsets:
                    self.topic_offsets[topic] = 0
                else:
                    self.topic_offsets[topic] += 1

                record = MockKafkaMessageRecord(topic=topic,
                                       offset=self.topic_offsets[topic],
                                       message_data=json.dumps(data),
                                       timestamp=int(timestamp.timestamp()*1000)
                )
                self.records.append(record)

    @property
    def topics(self):
        return list(self.topic_offsets.keys())

    def metrics(self):
        return {"message": "Test metrics"}

    def _fetch_all_topic_metadata(self):
        pass

    def assignment(self):
        return {TopicPartition(topic, 0) for topic in self.topic_offsets}

    def position(self, tp):
        return self.topic_offsets.get(tp.topic, 0)

    def highwater(self, tp):
        return self.topic_offsets.get(tp.topic, 0)

    def partitions_for_topic(self, topic):
        # tells _consume_topic_with_retry's startup-offset lookup there is
        # no history to search - only exercised by tests that go through
        # _run()/_consume_topic_with_retry rather than calling
        # consume_topic() directly
        return None

    def __iter__(self):
        return iter(self.records)

    def poll(self, timeout_ms=0, max_records=None):
        """
        consume_topic() drives the consumer via poll() (up to max_records,
        or up to timeout_ms of waiting) instead of the iterator protocol.
        This mock ignores both bounds and just hands back everything on
        every call (mirroring the old __iter__ behavior, which likewise
        replayed the same fixed records on every outer-loop pass), with a
        small sleep so consume_topic()'s loop doesn't spin unbounded.
        """
        if not self.records:
            return {}

        time.sleep(0.05)
        by_partition: dict[TopicPartition, list] = {}
        for record in self.records:
            by_partition.setdefault(TopicPartition(record.topic, 0), []).append(record)
        return by_partition

@pytest.mark.parametrize("txt,expected_topic,expected_lower_bound,expected_upper_bound",
    [
        ["topic-any","topic-any", None,None],
        ["topic-123","topic-123", None,None],
        ["node.sample:11","node.sample", 11,None],
        ["node.sample:21-55","node.sample", 21,55],
        ["node.sample:-55", None, None, None],
    ])
def test_MessageSubscriber_extract_offset_bounds(txt,
            expected_topic,
            expected_lower_bound,
            expected_upper_bound):

    topic_bound = None
    if expected_topic is None:
        with pytest.raises(ValueError):
            MessageSubscriber.extract_offset_bounds(txt)
    else:
        topic_bound = MessageSubscriber.extract_offset_bounds(txt)

        assert topic_bound.topic == expected_topic
        assert topic_bound.lower_bound == expected_lower_bound
        assert topic_bound.upper_bound == expected_upper_bound


@pytest.mark.asyncio(loop_scope="function")
@pytest.mark.parametrize("sonar_msg_files, expected_clusters",
    [
        [ ["0+job-srl-login3.ex3.simula.no.json"], {"ex3.simula.no": {"nodes": ['h001', 'n004', 'g001'], "partitions": ['dgx2q', 'habanaq', 'hgx2q', 'mi100q'] }}],
        [ ["4+cluster.fox.educloud.no.json"], {
              "fox.educloud.no": {
                'partitions': ['normal', 'hf_accel', 'bigmem', 'ifi_accel', 'fhi_bigmem', 'ifi_bigmem', 'autotekst', 'accel_long', 'accel', 'pods', 'dgx_spark', 'klm_accel', 'mig'],
                'nodes': ['c1-10', 'c1-16', 'c1-25', 'c1-21', 'gpu-10', 'gpu-12', 'gpu-6', 'gpu-17', 'c1-7', 'c1-27', 'c1-8', 'c1-12', 'gpu-13', 'gpu-7', 'dgx-1', 'c1-9', 'c1-20', 'c1-22', 'gpu-8', 'gpu-16', 'gpu-1', 'c1-5', 'c1-6', 'c1-28', 'c1-18', 'gpu-9', 'c1-11', 'c1-26', 'c1-23', 'gpu-4', 'c1-15', 'gpu-11', 'c1-14', 'c1-19', 'c1-17', 'dgx-2', 'c1-29', 'gpu-14', 'c1-24', 'gpu-5', 'bigmem-2', 'bigmem-1', 'gpu-2', 'gpu-15', 'c1-13']
            }}],
    ]
)
async def test_MessageSubscriber_sonar_examples(sonar_msg_files,
                                             expected_clusters,
                                             test_db_v2__function_scope,
                                             db_config,
                                             test_data_dir):
    db = test_db_v2__function_scope

    consumer = MockKafkaConsumer(sonar_msg_files, test_data_dir)
    message_subscriber = MessageSubscriber(host="localhost", port="9999",
                      topics=list(consumer.topic_offsets.keys()),
                      cluster_name=consumer.cluster,
                      database=db,
                      stats_interval_in_s=0,
    )

    message_handler = DBJsonImporter(db)

    task = asyncio.create_task(message_subscriber.consume_topic(
            topic=consumer.topics[0],
            consumer=consumer,
            msg_handler=message_handler
    ))

    await asyncio.sleep(3)
    message_subscriber.request_stop()
    await task

    for cluster_name, nodes in expected_clusters.items():
        with db.make_session() as session:
            results = session.execute(sqlalchemy.text(f"SELECT cluster, nodes, partitions, time from cluster_attributes where cluster = '{cluster_name}' ORDER BY time DESC")).all()
            assert results
            cluster, nodes, partitions, cluster_time = results[0]
            expected_nodes = expected_clusters[cluster_name]['nodes']
            expected_partitions = expected_clusters[cluster_name]['partitions']

            assert sorted(nodes) == sorted(expected_nodes), f"Expected nodes {expected_nodes} in cluster_attributes, but got {nodes=}"
            assert sorted(partitions) == sorted(expected_partitions), f"Expected partitions {expected_partitions} in cluster_attributes, but got {partitions=}"


@pytest.mark.asyncio(loop_scope="function")
async def test_MessageSubscriber_run_uses_one_db_connection_per_topic(
        test_db_v2__function_scope,
        db_config,
        test_data_dir,
        monkeypatch):
    """
    _run() spawns one consumer thread per topic (via
    _consume_topic_with_retry); each of those threads must construct its
    own ClusterDB rather than sharing the MessageSubscriber's, so a
    connection issue or a leak in one topic can't affect the others.
    """
    db = test_db_v2__function_scope

    # One topic per mock consumer, mirroring one file each - a shared
    # MockKafkaConsumer would reject messages from two different clusters
    # (see its cluster-mismatch check), same as a real subscription would
    # only ever see one topic.
    per_topic_consumer: dict[str, MockKafkaConsumer] = {}
    for sonar_msg_file in ["0+job-srl-login3.ex3.simula.no.json", "4+cluster.fox.educloud.no.json"]:
        consumer = MockKafkaConsumer([sonar_msg_file], test_data_dir)
        per_topic_consumer[consumer.topics[0]] = consumer

    def fake_kafka_consumer(topic, *args, **kwargs):
        return per_topic_consumer[topic]

    monkeypatch.setattr(message_subscriber_module, "KafkaConsumer", fake_kafka_consumer)

    created_dbs = []
    original_init = db.__class__.__init__

    def spy_init(self, db_settings):
        created_dbs.append(self)
        original_init(self, db_settings)

    monkeypatch.setattr(db.__class__, "__init__", spy_init)

    message_subscriber = MessageSubscriber(host="localhost", port="9999",
                      topics=list(per_topic_consumer.keys()),
                      cluster_name="unused-cluster-name",
                      database=db,
                      stats_interval_in_s=0,
    )

    task = asyncio.create_task(message_subscriber._run())
    await asyncio.sleep(3)
    message_subscriber.request_stop()
    await task

    assert len(created_dbs) == len(per_topic_consumer), (
        f"Expected one ClusterDB per topic ({len(per_topic_consumer)}), got {len(created_dbs)}"
    )
    assert len({id(created_db) for created_db in created_dbs}) == len(created_dbs), (
        "Expected a distinct ClusterDB instance per topic thread"
    )

    with db.make_session() as session:
        result = session.execute(sqlalchemy.text(
            "SELECT cluster FROM cluster_attributes WHERE cluster = 'fox.educloud.no'"
        )).all()
        assert result, "Expected the cluster-topic thread's own DB connection to have written its data"


@pytest.mark.asyncio(loop_scope="function")
async def test_MessageSubscriber_run_aggregates_per_topic_output(
        test_db_v2__function_scope,
        db_config,
        test_data_dir,
        monkeypatch):
    """
    _run()'s aggregator loop should surface each topic's own highlight
    (rather than just a single merged winner) and merge msg_timestamps
    across topics into the single public `output`.
    """
    db = test_db_v2__function_scope

    per_topic_consumer: dict[str, MockKafkaConsumer] = {}
    for sonar_msg_file in ["0+job-srl-login3.ex3.simula.no.json", "0+sample-g001.ex3.simula.no.json"]:
        consumer = MockKafkaConsumer([sonar_msg_file], test_data_dir)
        per_topic_consumer[consumer.topics[0]] = consumer

    def fake_kafka_consumer(topic, *args, **kwargs):
        return per_topic_consumer[topic]

    monkeypatch.setattr(message_subscriber_module, "KafkaConsumer", fake_kafka_consumer)

    message_subscriber = MessageSubscriber(host="localhost", port="9999",
                      topics=list(per_topic_consumer.keys()),
                      cluster_name="unused-cluster-name",
                      database=db,
                      stats_interval_in_s=0,
    )

    task = asyncio.create_task(message_subscriber._run())
    await asyncio.sleep(3)
    message_subscriber.request_stop()
    await task

    assert set(message_subscriber.output.highlights.keys()) == set(per_topic_consumer.keys()), (
        "Expected one highlight per topic, not just a single merged one"
    )
    # only the sample/sysinfo topics populate per-node timestamps - the job
    # topic here contributes none, but the sample topic's should still make
    # it through to the merged output
    assert message_subscriber.output.msg_timestamps, (
        "Expected the sample topic's per-node timestamps to have been merged into output"
    )
