import pytest
import asyncio
import datetime as dt
from pathlib import Path
import json
import sqlalchemy

from slurm_monitor.utils import utcnow
from slurm_monitor.db.v2.message_subscriber import MessageSubscriber
from slurm_monitor.db.v2.importer import DBJsonImporter

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
    class MockKafkaMessageRecord:
        topic: str
        offset: int = 0
        value: str
        timestamp: int

        def __init__(self, topic: str, message_data: str, offset: int = 0, timestamp: int = int(utcnow().timestamp()*1000)):
            self.topic = topic
            self.value = message_data.encode("UTF-8")
            self.offset = offset
            self.timestamp = timestamp

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
            return True

        def __iter__(self):
            return iter(self.records)

    db = test_db_v2__function_scope

    consumer = MockKafkaConsumer(sonar_msg_files)
    message_subscriber = MessageSubscriber(host="localhost", port="9999",
                      topics=list(consumer.topic_offsets.keys()),
                      cluster_name=consumer.cluster,
                      database=db,
                      stats_interval_in_s=0,
    )

    message_handler = DBJsonImporter(db)

    task = asyncio.create_task(message_subscriber.consume(
            topics=consumer.topics,
            consumer=consumer,
            msg_handler=message_handler
    ))

    await asyncio.sleep(3)
    message_subscriber.state = MessageSubscriber.State.STOPPING
    await task

    for cluster_name, nodes in expected_clusters.items():
        with db.make_session() as session:
            results = session.execute(sqlalchemy.text(f"SELECT cluster, nodes, partitions, time from cluster_attributes where cluster = '{cluster_name}' ORDER BY time DESC")).all()
            assert results
            cluster, nodes, partitions, time = results[0]
            expected_nodes = expected_clusters[cluster_name]['nodes']
            expected_partitions = expected_clusters[cluster_name]['partitions']

            assert sorted(nodes) == sorted(expected_nodes), f"Expected nodes {expected_nodes} in cluster_attributes, but got {nodes=}"
            assert sorted(partitions) == sorted(expected_partitions), f"Expected partitions {expected_partitions} in cluster_attributes, but got {partitions=}"
