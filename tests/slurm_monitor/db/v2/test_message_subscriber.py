import pytest
import asyncio
import datetime as dt
from pathlib import Path
import json
import sqlalchemy

from slurm_monitor.utils import utcnow
from slurm_monitor.db.v2.message_subscriber import MessageSubscriber
from slurm_monitor.db.v2.importer import DBJsonImporter


@pytest.mark.parametrize(
    "txt,expected_topic,expected_lower_bound,expected_upper_bound",
    [
        ["topic-any", "topic-any", None, None],
        ["topic-123", "topic-123", None, None],
        ["node.sample:11", "node.sample", 11, None],
        ["node.sample:21-55", "node.sample", 21, 55],
        ["node.sample:-55", None, None, None],
    ],
)
def test_MessageSubscriber_extract_offset_bounds(
    txt, expected_topic, expected_lower_bound, expected_upper_bound
):
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
@pytest.mark.parametrize(
    "sonar_msg_files, expected_clusters",
    [
        [
            ["0+job-srl-login3.ex3.simula.no.json"],
            {
                "ex3.simula.no": {
                    "nodes": ["h001", "n004", "g001"],
                    "partitions": ["dgx2q", "habanaq", "hgx2q", "mi100q"],
                }
            },
        ]
    ],
)
async def test_MessageSubcriber_sonar_examples(
    sonar_msg_files,
    expected_clusters,
    test_db_v2__function_scope,
    db_config,
    test_data_dir,
):
    class MockKafkaMessageRecord:
        topic: str
        offset: int = 0
        value: str

        def __init__(self, topic: str, message_data: str, offset: int = 0):
            self.topic = topic
            self.value = message_data.encode("UTF-8")
            self.offset = offset

    class MockKafkaConsumer:
        records: MockKafkaMessageRecord

        cluster: str | None
        topic_offsets: dict[str, int]

        def __init__(self, message_files: list[str]):
            self.cluster = None
            self.topic_offsets = {}
            self.records = []

            for sonar_msg_file in sonar_msg_files:
                json_filename = Path(test_data_dir) / "sonar" / sonar_msg_file
                with open(json_filename, "r") as f:
                    data = json.load(f)

                    cluster = data["data"]["attributes"]["cluster"]
                    data["data"]["attributes"]["time"] = (
                        utcnow() - dt.timedelta(hours=1)
                    ).isoformat()

                    if not self.cluster:
                        self.cluster = cluster
                    elif self.cluster != cluster:
                        raise RuntimeError(
                            f"Cluster expected message for one cluster, but got {self.cluster} and {cluster}"
                        )

                    topic = cluster + "." + data["data"]["type"]
                    if topic not in self.topic_offsets:
                        self.topic_offsets[topic] = 0
                    else:
                        self.topic_offsets[topic] += 1

                    record = MockKafkaMessageRecord(
                        topic=topic,
                        offset=self.topic_offsets[topic],
                        message_data=json.dumps(data),
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
            return True

        def __iter__(self):
            return iter(self.records)

    db = test_db_v2__function_scope

    consumer = MockKafkaConsumer(sonar_msg_files)
    message_subscriber = MessageSubscriber(
        host="localhost",
        port="9999",
        topics=list(consumer.topic_offsets.keys()),
        cluster_name=consumer.cluster,
        database=db,
    )

    message_handler = DBJsonImporter(db)

    task = asyncio.create_task(
        message_subscriber.consume(
            topics=consumer.topics, consumer=consumer, msg_handler=message_handler
        )
    )

    await asyncio.sleep(3)
    message_subscriber.state = MessageSubscriber.State.STOPPING
    await task

    for cluster_name, nodes in expected_clusters.items():
        with db.make_session() as session:
            results = session.execute(
                sqlalchemy.text(
                    f"SELECT cluster, nodes, partitions from cluster_attributes where cluster = '{cluster_name}'"
                )
            ).all()
            assert results
            cluster, nodes, partitions = results[0]
            expected_nodes = expected_clusters[cluster_name]["nodes"]
            expected_partitions = expected_clusters[cluster_name]["partitions"]

            assert sorted(nodes) == sorted(
                expected_nodes
            ), f"Expected nodes {expected_nodes} in cluster_attributes, but got {nodes=}"
            assert sorted(partitions) == sorted(
                expected_partitions
            ), f"Expected partitions {expected_partitions} in cluster_attributes, but got {partitions=}"
