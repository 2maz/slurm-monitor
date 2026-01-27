from __future__ import annotations

import asyncio
from enum import Enum
import logging
import datetime as dt
from kafka import KafkaConsumer, TopicPartition
import json
from pathlib import Path
import re
import time
import traceback as tb

from slurm_monitor.utils import utcnow
import slurm_monitor.db.v2.sonar as sonar
from slurm_monitor.db.v2.importer import (
    Importer,
    DBJsonImporter
)
from slurm_monitor.db.v2.db_tables import TableBase
from slurm_monitor.db.v2.db import (
    ClusterDB
)

logger = logging.getLogger(__name__)

KAFKA_CONSUMER_DEFAULTS = {
    'max_poll_records': 5000,
    'max_poll_interval_ms': '300000',
    'fetch_max_bytes': 200*1024**2,
    'max_partition_fetch_bytes': 200*1024**2
}

LOOKBACK_IN_H_DEFAULT = 36
LOOKBACK_IN_H_DEFAULTS : dict[str, float] = {
    sonar.TopicType.cluster: LOOKBACK_IN_H_DEFAULT,
    sonar.TopicType.sample: 1,
    sonar.TopicType.job: 1,
    sonar.TopicType.sysinfo: LOOKBACK_IN_H_DEFAULT,
}

class TopicBound:
    topic: str
    lower_bound: int | None
    upper_bound: int | None

    def __init__(self, topic: str,
                 lower_bound: int | None,
                 upper_bound: int | None):
        self.topic = topic

        self.lower_bound = lower_bound
        self.upper_bound = upper_bound

class MessageSubscriber:
    host: str
    port: int
    cluster_name: str
    topics: list[str] | None
    database: ClusterDB | None
    retry_timeout_in_s: int
    verbose: bool
    strict_mode: bool

    lookback_in_h: dict[str, float]
    kafka_consumer_options: dict[str, any]

    state: MessageSubscriber.State

    class State(str, Enum):
        INITIALIZING = 'INITIALIZING'
        RUNNING = 'RUNNING'
        STOPPING = 'STOPPING'
        UNKNOWN = 'UNKNOWN'

    def __init__(self,
            host: str, port: int,
            cluster_name: str,
            topics: str | list[str] | None,
            database: ClusterDB | None = None,
            retry_timeout_in_s: int = 5,
            verbose: bool = False,
            strict_mode: bool = False,
            lookback_in_h: dict[str, float] = LOOKBACK_IN_H_DEFAULTS,
            kafka_consumer_options: dict[str, any] = KAFKA_CONSUMER_DEFAULTS,
            stats_output: Path | str | None = None,
            stats_interval_in_s: int = 30
    ):
        self.host = host
        self.port = port
        self.cluster_name = cluster_name

        self.stats_output = stats_output
        self.stats_interval_in_s = stats_interval_in_s

        if not cluster_name:
            raise ValueError("MessageSubscriber.__init__: cluster_name required")

        if type(topics) is str:
            self.topics = [topics]
        else:
            self.topics = topics
        self.database = database
        self.retry_timeout_in_s = retry_timeout_in_s
        self.verbose = verbose
        self.strict_mode = strict_mode
        self.lookback_in_h = lookback_in_h
        self.kafka_consumer_options = kafka_consumer_options

        self.state = self.State.UNKNOWN

    @classmethod
    def extract_lookback(cls, lookback: str) -> tuple[str, float]:
        """
            return tuple of topic name and hours
        """
        m = re.match(r"[0-9]+(\.[0-9]+)?", lookback)
        if m:
            return None, float(lookback)

        m = re.match(r"^([^:]+):([0-9]+(\.[0-9]+)?)$", lookback)
        if m:
            return m.groups()[0], float(m.groups()[1])

        raise ValueError(f"Invalid pattern: {lookback} - could not extract lookback")


    @classmethod
    def extract_lookbacks(cls, lookbacks: list[str]) -> dict[str, float]:
        lookbacks_in_h : dict[sonar.TopicType, float] = LOOKBACK_IN_H_DEFAULTS.copy()

        if not lookbacks:
            return lookbacks_in_h

        for lookback in lookbacks:
            topic_name, lookback_in_h = cls.extract_lookback(lookback)
            if topic_name:
                topic = getattr(sonar.TopicType, topic_name)
                lookbacks_in_h[topic] = lookback_in_h
            else:
                # setting all the defaults
                for x in lookbacks_in_h:
                    lookbacks_in_h[x] = lookback_in_h

        return lookbacks_in_h


    async def consume(self,
                topics: list[str],
                consumer: KafkaConsumer,
                msg_handler: Importer,
                startup_offsets: dict[str, int] = {},
                topic_lb: dict[str, int] = {},
                topic_ub: dict[str,int] = {}
            ):

        start_time = dt.datetime.now(dt.timezone.utc)

        if msg_handler is None:
            raise ValueError("MessageHandler must be given")

        ignore_integrity_errors = False
        if startup_offsets:
            ignore_integrity_errors = True
        else:
            self.state = self.State.RUNNING

        while topics and not self.state == self.State.STOPPING:
            interval_start_time = dt.datetime.now(dt.timezone.utc)

            consumer._fetch_all_topic_metadata()
            if not consumer.assignment():
                # Wait for partitions to become available
                time.sleep(5)
                continue

            for idx, consumer_record in enumerate(consumer, 1):
                try:
                    topic = consumer_record.topic
                    if topic in topics:
                        if topic in startup_offsets:
                            so = startup_offsets[topic]
                            if so and consumer_record.offset >= so:
                                del startup_offsets[topic]

                        if topic in topic_ub:
                            ub = topic_ub[topic]
                            if ub and consumer_record.offset >= ub:
                                print(f"MessageSubscriber.consume: {topic.ljust(25)} -- upper bound reached: {ub}, pausing: {topic}")
                                consumer.pause(TopicPartition(topic, 0))
                                topics.remove(topic)
                                if not topics:
                                    print("MessageSubscriber.consume: no topics left to listen on. Stopping ...")
                                    self.state = self.State.STOPPING
                                    return

                    if self.state == self.State.INITIALIZING and not startup_offsets:
                        print(f"Startup completed: historic message lookup finished (after {(utcnow() - start_time).total_seconds():.2}s)")
                        self.state = self.State.RUNNING
                        ignore_integrity_errors = False

                    msg = consumer_record.value.decode("UTF-8")
                    if self.verbose:
                        print(msg)

                    # If a sample arrives there should be no duplicates in the database - an exception is the initialization
                    # where historic records are retrieved
                    if sonar.TopicType.infer(topic) == sonar.TopicType.sample:
                        await msg_handler.insert(json.loads(msg), update=False, ignore_integrity_errors=ignore_integrity_errors)
                    else:
                        # Allow to update / merge existing information
                        await msg_handler.insert(json.loads(msg), update=True, ignore_integrity_errors=ignore_integrity_errors)

                    if sonar.TopicType.infer(topic) == sonar.TopicType.job:
                        logging.info("Auto update - aligning cluster information from jobs data")
                        await msg_handler.autoupdate(cluster=self.cluster_name)

                    now = utcnow()
                    seconds_from_now = (now.timestamp() - consumer_record.timestamp/1000.0)
                    print(f"[{self.state.value}][{now.isoformat(timespec='milliseconds')}] last processed: topic={topic} offset={consumer_record.offset} latency: {seconds_from_now:.2f}s       \r",
                          flush=True,
                          end=''
                    )

                    if (dt.datetime.now(dt.timezone.utc) - interval_start_time).total_seconds() > self.stats_interval_in_s:
                        interval_start_time = dt.datetime.now(dt.timezone.utc)

                        msg_timestamps = msg_handler.last_msg_per_node.values()
                        max_delay = 0
                        if msg_timestamps:
                            max_delay = (interval_start_time - min(msg_handler.last_msg_per_node.values())).total_seconds()
                            print(f"\n\nLast known messages - {interval_start_time} - {max_delay=} s")
                            for node_num, node in enumerate(sorted(msg_handler.last_msg_per_node.keys())):
                                print(f"{node_num:03} {node.ljust(20)} {msg_handler.last_msg_per_node[node]}")
                        else:
                            print(f"\n\nNo messages received - {interval_start_time} s")

                        metrics = consumer.metrics()
                        listen_status = {
                                         'positions': { },
                                         'stats_interval_in_s': self.stats_interval_in_s,
                                         'interval_start_time': interval_start_time,
                                         'max_delay': max_delay
                                        }

                        consumer._fetch_all_topic_metadata()
                        for tp in consumer.assignment():
                            current_pos = consumer.position(tp)
                            highwater = consumer.highwater(tp)
                            print(f"Partition: {tp.topic.ljust(25)} -- {current_pos} / {highwater}")

                            listen_status['positions'][tp.topic] = { 'current': current_pos, 'highwater': highwater }

                            # Include startup cleanup for topics that have received no updates
                            if tp.topic in startup_offsets:
                                so = startup_offsets[tp.topic]
                                if so and current_pos >= so:
                                    del startup_offsets[tp.topic]

                        metrics['listen'] = listen_status
                        stats = json.dumps(metrics, indent=4, default=str)
                        print(stats)

                        if self.stats_output:
                            stats_output = Path(self.stats_output)
                            stats_output.parent.mkdir(parents=True, exist_ok=True)

                            with open(stats_output, "w") as f:
                                f.write(stats)

                except Exception as e:
                    if self.verbose:
                        tb.print_tb(e.__traceback__)

                    logger.warning(f"Message processing failed: {e}")

    @classmethod
    def extract_offset_bounds(cls, txt) -> TopicBound:
        # check if topic follows: <topic-name>:<lower-bound-offset>-<upper-bound-offset>
        m = re.match(r"^([^:]+)(:[0-9]+)?(-[0-9]+)?$", txt)
        if not m:
            raise ValueError("MessageSubscriber: invalid pattern: use <topic-name>, or <topic-name>:<lower-bount:int> or <topic-name>:<lower-bound:int>-<upper-bound:int>")

        topic = m.groups()[0]
        lower_bound = None
        upper_bound = None
        for g in m.groups()[1:]:
            if not g:
                continue

            if g.startswith(":"):
                lower_bound = int(g[1:])
            elif g.startswith("-"):
                upper_bound = int(g[1:])

        return TopicBound(topic, lower_bound, upper_bound)

    def run(self):
        asyncio.run(self._run())

    async def _run(self):
        """
        Set up a kafka consumer that subscribes to a list of topics

        Note that a topic can be defined with a lower bound and and upper bound offset, e.g., as "<topic_name>:<lb-offset>-<ub-offset>.
            - when an lower bound offset is defined: start the consumption of messages for the related topic at this message offset
            - when an upper bound offset is defined: end the consumption of messages for the related topic, when a (topic) message with an offset equal or larger than this bound is encountered.
        """

        if self.strict_mode:
            TableBase.__extra_values__ = 'forbid'

        msg_handler = Importer()
        if self.database:
            msg_handler = DBJsonImporter(db=self.database)
        else:
            print("MessageSubscriber: no database specified. Will only print messages to console")

        topic_lb = {}
        topic_ub = {}

        topics = self.topics
        if not topics:
            topics = [f"{x.get_topic(cluster=self.cluster_name)}" for x in sonar.TopicType]
        else:
            # Process topic will contain the topics without and lower bound, upper bound constraints
            processed_topics = []
            for t in topics:
                topic_bound = self.extract_offset_bounds(t)
                if topic_bound.lower_bound is not None:
                    topic_lb[topic_bound.topic] = topic_bound.lower_bound
                if topic_bound.upper_bound is not None:
                    topic_ub[topic_bound.topic] = topic_bound.upper_bound
                processed_topics.append(topic_bound.topic)

            topics = processed_topics

        self.state = self.State.INITIALIZING
        while self.state != self.State.STOPPING:
            try:
                # https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html
                logger.info(f"Subscribing to topics: {topics}")
                consumer = KafkaConsumer(
                            *topics,
                            bootstrap_servers=f"{self.host}:{self.port}",
                            **self.kafka_consumer_options
                            )
                consumer._fetch_all_topic_metadata()

                # In particular sysinfo message are expected to run with low
                # cadence (every 24\,h)
                # While the default KafkaConsumer seeks to the end of the partition
                # sysinfo message might not appear for hours.
                # Hence, in cases where sysinfo message have been already recorded
                # ensure that the listener picks them up
                startup_offsets = {}
                for topic in topics:
                    if topic not in topic_lb:
                        tp = TopicPartition(topic, 0)
                        if not consumer.partitions_for_topic(topic):
                            continue

                        # go back in history to search for topic messages
                        topic_type = sonar.TopicType.infer(topic=topic)
                        timelimit = utcnow() - dt.timedelta(seconds=int(self.lookback_in_h.get(topic_type, LOOKBACK_IN_H_DEFAULT)*3600))
                        logger.info(f"{topic=}: search offset for {timelimit=}")
                        offset_and_timestamp = consumer.offsets_for_times({ tp: int(timelimit.timestamp()*1000) })[tp]
                        if not offset_and_timestamp:
                            continue

                        offset = offset_and_timestamp.offset
                        timestamp_in_s = offset_and_timestamp.timestamp / 1000
                        logger.info(f"{topic=}: found {offset=} for {dt.datetime.fromtimestamp(timestamp_in_s)}")
                        topic_lb[topic] = offset
                        startup_offsets[topic] = consumer.end_offsets([tp])[tp]

                for topic, lb in topic_lb.items():
                    logger.info(f"{topic=}: seek to {lb}")
                    consumer.seek(TopicPartition(topic, 0), lb)

                await self.consume(topics, consumer,
                              msg_handler=msg_handler,
                              startup_offsets=startup_offsets,
                              topic_lb=topic_lb, topic_ub=topic_ub)
            except TimeoutError:
                raise
            except Exception as e:
                logger.warning(f"Connection failed - retrying in 5s - {e}")
                time.sleep(self.retry_timeout_in_s)

        logger.info("All tasks gracefully stopped")
