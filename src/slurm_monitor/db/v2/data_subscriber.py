from kafka import KafkaConsumer, TopicPartition
from slurm_monitor.db.v2.db_tables import (
    Cluster,
    ErrorMessage,
    Node,
    NodeState,
    Partition,
    SampleGpu,
    SampleProcess,
    SampleProcessGpu,
    SampleSlurmJob,
    SampleSlurmJobAcc,
    SampleSystem,
    SysinfoAttributes,
    SysinfoGpuCard,
    SysinfoGpuCardConfig,
    TableBase
)
from slurm_monitor.db.v2.db import (
    ClusterDB
)

import dataclasses
import datetime as dt
import json
import logging
import re
import time
import traceback as tb

logger = logging.getLogger(__name__)


last_msg_per_node = {}

@dataclasses.dataclass
class Meta:
    producer: str
    version: str

@dataclasses.dataclass
class Data:
    type: str
    attributes: dict[str, any]

@dataclasses.dataclass
class Message:
    meta: Meta
    data: Data | None
    errors: list[ErrorMessage] | None




def expand_node_names(names: str) -> list[str]:
    nodes = []

    if type(names) is str:
        names = names.replace("],","];")
        names = names.split(';')

    for pattern in names:
        m = re.match(r"(.*)\[(.*)\]$", pattern)
        if m is None:
            nodes.append(pattern)
            continue

        prefix, suffixes = m.groups()
        # [005-006,001,005]
        for suffix in suffixes.split(','):
            #0,0
            if "-" not in suffix:
                nodes.append(prefix + suffix)
                continue

            # 001-010
            start, end = suffix.split("-")
            pattern_length = len(start)

            for i in range(int(start), int(end)+1):
                node_number = str(i).zfill(pattern_length)
                nodes.append(prefix + node_number)

    return nodes


class DBJsonImporter:
    db: ClusterDB
    def __init__(self, db: ClusterDB):
        self.db = db

    @classmethod
    def to_message(cls, message: dict[str, any]) -> Message:
        if "meta" not in message:
            raise ValueError(f"Missing 'meta' in {message=}")

        meta = Meta(**message['meta'])

        data = None
        if "data" not in message and "errors" not in message:
            raise ValueError(f"Either 'data' or 'errors' must be present in {message=}")

        if 'data' in message:
            data = Data(**message['data'])


        errors = None
        if 'errors' in message:
            errors = [ErrorMessage(**x) for x in message['errors']]

        return Message(meta=meta, data=data, errors=errors)

    @classmethod
    def parse(cls, message: dict[str, any]):
        """
        Parse a message a trigger parsing according to the respective message type

        Will call DBJsonImporter.parse_<msgtype> function to handle a message.
        """
        msg = DBJsonImporter.to_message(message)

        msg_type = "errors"
        if not msg.errors:
            msg_type = msg.data.type

        if not hasattr(cls, f"parse_{msg_type}"):
            raise NotImplementedError(f"DBJsonImporter.parse: no parser for message type: {msg_type} implemented")

        parser_fn = getattr(cls, f"parse_{msg_type}")
        return parser_fn(msg)


    @classmethod
    def parse_sysinfo(cls, msg: Message) -> list[TableBase | list[TableBase]]:
        """
        Parse messages of type 'sysinfo'
        """
        attributes = msg.data.attributes
        cluster = attributes.get('cluster', '')
        node = attributes['node']

        time = dt.datetime.fromisoformat(attributes["time"])
        del attributes['time']

        gpu_uuids = []
        gpu_info = []
        if 'cards' in attributes:

            cards = attributes['cards']
            del attributes['cards']

            gpu_cards = []
            gpu_card_configs = []

            for card in cards:
                data = {}
                for field in ["manufacturer", "model", "architecture", "memory"]:
                    if field in card:
                        data[field] = card[field]
                        del card[field]

                gpu_uuid = card['uuid']
                if gpu_uuid is None or gpu_uuid == '':
                    logger.debug(f"SysInfo: skipping sample from {cluster=} {node=} due to missing 'uuid' {card=}")
                    continue

                gpu_cards.append(SysinfoGpuCard(
                        uuid=gpu_uuid,
                        **data,
                    )
                )

                gpu_card_configs.append(SysinfoGpuCardConfig(
                        cluster=cluster,
                        node=node,
                        time=time,
                        **card
                    )
                )
                gpu_uuids.append(gpu_uuid)

            gpu_info.append(gpu_cards)
            gpu_info.append(gpu_card_configs)

        node = Node(
                cluster=attributes.get('cluster', ''),
                node=attributes['node'],
                architecture=attributes['architecture']
        )

        sysinfo = SysinfoAttributes(
                    time=time,
                    cards=gpu_uuids,
                    **attributes
                )

        return [node, sysinfo] + gpu_info

    @classmethod
    def parse_sample(cls, msg: Message) -> list[TableBase | list[TableBase]]:
        """
        Parse messages of type 'sample'
        """
        attributes = msg.data.attributes
        time = dt.datetime.fromisoformat(attributes["time"])
        del attributes['time']

        cluster = attributes.get('cluster','')
        system = attributes["system"]
        node = attributes['node']

        last_msg_per_node[node] = time

        gpu_samples = []
        if "gpus" in system:
            gpus = system["gpus"]
            del system["gpus"]

            for gpu in gpus:
                if 'uuid' not in gpu:
                    logger.debug(f"Sample: skipping sample due to missing 'uuid' {gpu=}")
                    continue

                gpu_status = SampleGpu(
                        **gpu,
                        time=time
                )
                gpu_samples.append(gpu_status)

        # consume remaining items from SampleSystem
        sample_system = SampleSystem(
                cluster=cluster,
                node=node,
                **system,
                time=time
        )

        jobs = attributes["jobs"]
        process_stati = []
        gpu_card_process_stati = []
        for job in jobs:
            job_id = job['job']

            user = job['user']
            epoch = job.get('epoch', 0)

            for process in job["processes"]:
                pid = process['pid']

                if "gpus" in process:
                    for gpu_data in process['gpus']:
                        gpu_card_process_stati.append(
                            SampleProcessGpu(
                                cluster=cluster,
                                node=node,
                                pid=pid,
                                job=job_id,
                                user=user,
                                epoch=epoch,
                                time=time,
                                **gpu_data
                            )
                        )
                    del process["gpus"]

                process_stati.append( SampleProcess(
                   cluster=cluster,
                   node=node,
                   job=job_id,
                   user=user,
                   epoch=epoch,
                   time=time,

                   **process)
                )
        return [gpu_samples, gpu_card_process_stati, process_stati, sample_system]

    @classmethod
    def parse_cluster(cls, msg: Message) -> list[TableBase | list[TableBase]]:
        """
        Parse messages of type 'cluster'
        """
        attributes = msg.data.attributes
        cluster_id = attributes.get('cluster', '')
        slurm = attributes['slurm']

        time = dt.datetime.fromisoformat(attributes["time"])
        del attributes['time']


        nodes = set()
        nodes_states = []
        for n in attributes['nodes']:
            node_names = expand_node_names(n['names'])
            nodes.update(node_names)

            states = n['states']
            for n in node_names:
                nodes_states.append(NodeState(
                        cluster=cluster_id,
                        node=n,
                        states=states,
                        time=time
                    )
                )

        partitions = []
        for p in attributes['partitions']:
            node_names = expand_node_names(p["nodes"])
            partitions.append(Partition(
                    cluster=cluster_id,
                    partition=p["name"],
                    nodes=node_names,
                    nodes_compact=p["nodes"],
                    time=time
                )
            )

        cluster = Cluster(
            cluster=cluster_id,
            slurm=slurm,
            partitions=[x.partition for x in partitions],
            nodes=list(nodes),
            time=time
        )

        cluster_nodes = [Node(cluster=cluster_id, node=x) for x in nodes]

        return [ cluster ]  + partitions + cluster_nodes + nodes_states

    @classmethod
    def parse_job(cls, msg: Message) -> list[TableBase | list[TableBase]]:
        """
        Parse messages of type 'job'
        """
        attributes = msg.data.attributes
        cluster = attributes.get('cluster', '')
        slurm_jobs = attributes['slurm_jobs']

        time = dt.datetime.fromisoformat(attributes["time"])
        del attributes['time']


        slurm_job_samples = []
        for job_data in slurm_jobs:
            sacct = None
            if 'sacct' in job_data:
                sacct = job_data['sacct']
                del job_data['sacct']

            slurm_job_samples.append(
                SampleSlurmJob(
                    cluster=cluster,
                    **job_data,
                    time=time
                )
            )
            if sacct:
                if 'job_step' in job_data:
                    sacct['job_step'] = job_data['job_step']

                slurm_job_samples.append(
                    SampleSlurmJobAcc(
                        cluster=cluster,
                        job_id=job_data['job_id'],
                        **sacct,
                        time=time
                    )
                )
        return slurm_job_samples

    @classmethod
    def parse_errors(cls, msg: Message) -> list[TableBase | list[TableBase]]:
        """
        Parse messages of type 'errors'
        """
        error_messages = []
        for error in msg.errors:
            error_messages.append(ErrorMessage(**error))
        return error_messages

    def insert(self, message: dict[str, any], update: bool = True):
        samples = DBJsonImporter.parse(message)
        for sample in samples:
            if sample:
                try:
                    if update:
                        self.db.insert_or_update(sample)
                    else:
                        self.db.insert(sample)
                except Exception as e:
                    logger.warning(f"Inserting sample {sample} failed. -- {e}")

def main(*,
        host: str, port: int,
        cluster_name: str,
        topics: str | list[str] | None,
        database: ClusterDB | None = None,
        retry_timeout_in_s: int = 5,
        verbose: bool = False,
        strict_mode: bool = False
        ):
    """
    Set up a kafka consumer that subscribes to a list of topics

    Note that a topic can be defined with a lower bound and and upper bound offset, e.g., as "<topic_name>:<lb-offset>-<ub-offset>.
    When an offset is define, the consumption of messages will stop as soon for that given topic.
    """

    if type(topics) is str:
        topics = [topics]

    msg_handler = None
    if database:
        msg_handler = DBJsonImporter(db=database)
    else:
        print("No database specified. Will only run in plain listen mode")

    topic_lb = {}
    topic_ub = {}
    if not topics:
        topics = [f"{cluster_name}.{x}" for x in ['cluster', 'job', 'sample', 'sysinfo']]
    else:
        processed_topics = []
        for t in topics:
            m = re.match(r"^([^-:]+)(:[0-9]+)?(-[0-9]+)?",t)
            if m:
                topic = m.groups()[0]
                processed_topics.append(topic)
                for g in m.groups()[1:]:
                    if not g:
                        continue

                    if g.startswith(":"):
                        topic_lb[topic] = int(g[1:])
                    elif g.startswith("-"):
                        topic_ub[topic] = int(g[1:])
        topics = processed_topics

    while True:
        try:
            # https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html
            logger.info(f"Subscribing to topics: {topics}")
            if not topic_lb:
                consumer = KafkaConsumer(
                            *topics,
                            bootstrap_servers=f"{host}:{port}",
                            max_poll_records=1000,
                            fetch_max_bytes=200*1024**2,
                            max_partition_fetch_bytes=200*1024**2,
                            )
            else:
                consumer = KafkaConsumer(
                            bootstrap_servers=f"{host}:{port}",
                            max_poll_records=1000,
                            fetch_max_bytes=200*1024**2,
                            max_partition_fetch_bytes=200*1024**2,
                            )

            start_time = dt.datetime.now(dt.timezone.utc)
            for topic, lb in topic_lb.items():
                consumer.assign([TopicPartition(topic, 0)])
                consumer.seek(TopicPartition(topic, 0), lb)

            while topics:
                interval_start_time = dt.datetime.now(dt.timezone.utc)
                for idx, consumer_record in enumerate(consumer, 1):
                    try:
                        if msg_handler:
                            topic = consumer_record.topic
                            if topic in topics and topic in topic_ub:
                                ub = topic_ub[topic]
                                if consumer_record.offset >= ub:
                                    print(f"Partition: {topic.ljust(25)} -- upper bound reached: {ub}, pausing: {topic}")
                                    consumer.pause([TopicPartition(topic, 0)])
                                    topics.remove(topic)

                            msg = consumer_record.value.decode("UTF-8")
                            if verbose:
                                print(msg)

                            if topic.endswith("sample"):
                                msg_handler.insert(json.loads(msg), update=False)
                            else:
                                msg_handler.insert(json.loads(msg), update=True)

                            print(f"{dt.datetime.now(dt.timezone.utc)} messages consumed: "
                                  f"{idx} since {start_time}\r",
                                  flush=True,
                                  end=''
                            )
                        else:
                            print(msg.value.decode("UTF-8"))

                        if (dt.datetime.now(dt.timezone.utc) - interval_start_time).total_seconds() > 60:
                            interval_start_time = dt.datetime.now(dt.timezone.utc)
                            max_delay = (interval_start_time - min(last_msg_per_node.values())).total_seconds()
                            print(f"\n\nLast known messages - {interval_start_time} - {max_delay=} s")
                            for node_num, node in enumerate(sorted(last_msg_per_node.keys())):
                                print(f"{node_num:03} {node.ljust(20)} {last_msg_per_node[node]}")

                            print(json.dumps(consumer.metrics(), indent=4))
                            for topic in topics:
                                tp = TopicPartition(topic, 0)
                                current_pos = consumer.position(tp)

                                highwater = consumer.highwater(tp)
                                print(f"Partition: {topic.ljust(25)} -- {current_pos} / {highwater}")

                    except Exception as e:
                        tb.print_tb(e.__traceback__)
                        logger.warning(f"Message processing failed: {e}")
        except TimeoutError:
            raise
        except Exception as e:
            logger.warning(f"Connection failed - retrying in 5s - {e}")
            time.sleep(retry_timeout_in_s)

    logger.info("All tasks gracefully stopped")
