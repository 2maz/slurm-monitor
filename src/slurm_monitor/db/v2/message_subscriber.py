from __future__ import annotations

import asyncio
import curses
from enum import Enum
from operator import itemgetter
import logging
from logging.handlers import QueueHandler, TimedRotatingFileHandler
import collections
import datetime as dt
from kafka import KafkaConsumer, TopicPartition
import json
from pathlib import Path
import re
import time
from typing import Iterable
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

from slurm_monitor.config import (
    SLURM_MONITOR_LOG_FORMAT,
    SLURM_MONITOR_LOG_STYLE,
    SLURM_MONITOR_LOG_DATE_FORMAT
)

logger = logging.getLogger(__name__)
logger.propagate = False

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
    output: Output

    class State(str, Enum):
        INITIALIZING = 'INITIALIZING'
        RUNNING = 'RUNNING'
        STOPPING = 'STOPPING'
        UNKNOWN = 'UNKNOWN'

    class Output:
        messages: Iterable[str]
        stats: dict[str, any]
        next_stats_update: int
        highlight: str

        max_msg_delay: int
        msg_timestamps: dict[str, dt.datetime]

        def __init__(self, parent: MessageSubscriber):
            self.parent = parent

            self.messages = collections.deque(maxlen=100)
            self.stats = {}
            self.highlight = ''

            self._screen = None
            self.next_stats_update = -1
            self.msg_timestamps = {}

        def put_nowait(self, record: logging.LogRecord):
            self.messages.append(record.message)

        def print(self):
            try:
                if not self._screen:
                    self._screen = curses.initscr()
                    self._screen.clear()
                    curses.noecho()
                    curses.cbreak()
                    self._screen.nodelay(True)

                self._screen.erase()
                screenwidth = 120

                self._screen.addstr(0, 0, f"{'-'*screenwidth}")
                self._screen.addstr(1, 0, ">> Status: slurm-monitor listen")
                self._screen.addstr(2, 0, "   q to quit | l to change log level")
                self._screen.addstr(3, 0, f"{'-'*screenwidth}")

                self._screen.addstr(5, 0, self.highlight, curses.A_BOLD)

                # Messages
                y_offset = 7
                self._screen.addstr(y_offset,   0, f"{'-'*screenwidth}")
                self._screen.addstr(y_offset+1, 0, f"| Messages (log level: {logging.getLevelName(logger.handlers[0].level)})")
                self._screen.addstr(y_offset+2, 0, f"{'-'*screenwidth}")

                idx = 0
                y_offset += 4
                for idx, msg in enumerate(list(self.messages)[-20:]):
                    self._screen.addstr(idx + y_offset, 0, msg)

                # Nodes
                y_offset += idx + 4
                # sort by time, then by name
                timesorted_messages = sorted([(k,v) for k,v in self.msg_timestamps.items()],key=itemgetter(1,0))
                self._screen.addstr(y_offset,   0, f"{'-'*screenwidth}")
                self._screen.addstr(y_offset+1, 0, "| Nodes")
                self._screen.addstr(y_offset+2, 0, f"{'-'*screenwidth}")

                y_offset += 3
                self._screen.addstr(y_offset, 0, "Recently seen first", curses.A_BOLD)
                for idx, msg in enumerate(reversed(timesorted_messages[-15:])):
                    self._screen.addstr(y_offset + idx + 1, 0, f"{msg[1]} {msg[0]}")
                self._screen.addstr(y_offset + idx + 2, 0, "    ...")

                self._screen.addstr(y_offset, 40, "Oldest seen first", curses.A_BOLD)
                for idx, msg in enumerate(timesorted_messages[:15]):
                    self._screen.addstr(y_offset + idx + 1, 40, f"{msg[1]} {msg[0]}")
                self._screen.addstr(y_offset + idx + 2, 40, "    ...")


                # Statistics
                y_offset += 19
                self._screen.addstr(y_offset,   0, f"{'-'*screenwidth}")
                self._screen.addstr(y_offset+1, 0, f"| Statistics (update in: {self.next_stats_update:.2f} s)")
                self._screen.addstr(y_offset+2, 0, f"{'-'*screenwidth}")

                if self.stats:
                    group_header_y = y_offset + 3
                    group_sizes = []
                    columns = 3

                    column_width = 40 # characters
                    for group_idx, (group_name, values) in enumerate(self.stats.items()):
                        group_header_x = (group_idx % columns)*column_width

                        # when to reset the group_header_y, so to progress to the next 'row' of groups
                        if group_sizes and group_idx % columns == 0:
                            group_header_y += max(group_sizes) + 4
                            group_sizes = []

                        # Group Header
                        self._screen.addstr(group_header_y, group_header_x, group_name, curses.A_BOLD)
                        # Properties
                        for field_idx, (field_name, field_value) in enumerate(values.items()):
                            if type(field_value) is float:
                                field_value = f"{field_value:.5f}"

                            try:
                                self._screen.addstr(group_header_y + 2 + field_idx, group_header_x, f"{field_name}: {field_value}", curses.A_DIM)
                            except curses.error:
                                pass

                        group_sizes.append(len(values))

                key = self._screen.getch()
                if key == ord('q'):
                    self.parent.state = MessageSubscriber.State.STOPPING
                    self._screen.addstr(0,0, "Received user's request to stop ... ]")
                elif key == ord('l'):
                    for handler in logger.handlers:
                        current_level = handler.level
                        if current_level == logging.CRITICAL:
                            handler.setLevel(logging.getLevelName(logging.NOTSET))
                        else:
                            # see https://docs.python.org/3/library/logging.html#loggin.NOTSET
                            handler.setLevel(logging.getLevelName(current_level+10))

                self._screen.refresh()
            except Exception as e:
                print("Screen update failed: {e}")
                tb.print_tb(e.__traceback__)

        def close(self):
            if self._screen:
                self._screen.clear()
                curses.echo()
                curses.nocbreak()
                curses.endwin()

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
            stats_interval_in_s: int = 30,
            log_output: Path | str | None = None,
            log_level: int = logging.INFO,
    ):
        self.host = host
        self.port = port
        self.cluster_name = cluster_name

        self.stats_output = stats_output
        self.stats_interval_in_s = stats_interval_in_s

        self.log_output = log_output
        self.log_level = log_level

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

        self.output = MessageSubscriber.Output(parent=self)

        # setup the logging
        formatter = logging.Formatter(
            fmt=SLURM_MONITOR_LOG_FORMAT,
            datefmt=SLURM_MONITOR_LOG_DATE_FORMAT,
            style=SLURM_MONITOR_LOG_STYLE
        )

        queue_handler = QueueHandler(self.output)
        queue_handler.setLevel(logging.getLevelName(log_level))
        queue_handler.setFormatter(formatter)
        logger.addHandler(queue_handler)

        if self.log_output:
            file_handler = TimedRotatingFileHandler(self.log_output, when='d', interval=3)
            file_handler.setLevel(logging.getLevelName(log_level))
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)


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
                                logger.info(f"MessageSubscriber.consume: {topic.ljust(25)} -- upper bound reached: {ub}, pausing: {topic}")
                                consumer.pause(TopicPartition(topic, 0))
                                topics.remove(topic)
                                if not topics:
                                    logger.info("MessageSubscriber.consume: no topics left to listen on. Stopping ...")
                                    self.state = self.State.STOPPING
                                    return

                    if self.state == self.State.INITIALIZING and not startup_offsets:
                        logger.info(f"Startup completed: historic message lookup finished (after {(utcnow() - start_time).total_seconds():.2}s)")
                        self.state = self.State.RUNNING
                        ignore_integrity_errors = False

                    if self.state == self.State.STOPPING:
                        break

                    msg = consumer_record.value.decode("UTF-8")
                    if self.verbose:
                        logger.info(f"Message: {msg}")

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

                    self.output.highlight = f"[{self.state.value}][{now.isoformat(timespec='milliseconds')}] last processed: topic={topic} offset={consumer_record.offset} latency: {seconds_from_now:.2f}s"

                    self.output.next_stats_update = self.stats_interval_in_s - (dt.datetime.now(dt.timezone.utc) - interval_start_time).total_seconds()
                    if (dt.datetime.now(dt.timezone.utc) - interval_start_time).total_seconds() > self.stats_interval_in_s:
                        interval_start_time = dt.datetime.now(dt.timezone.utc)

                        msg_timestamps = msg_handler.last_msg_per_node
                        max_delay = 0
                        if msg_timestamps:
                            max_delay = (interval_start_time - min(msg_handler.last_msg_per_node.values())).total_seconds()
                            self.output.msg_timestamps = msg_timestamps
                        else:
                            logger.warning(f"No messages received - {interval_start_time} s")

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
                            listen_status['positions'][tp.topic] = { 'current': current_pos, 'highwater': highwater }

                            # Include startup cleanup for topics that have received no updates
                            if tp.topic in startup_offsets:
                                so = startup_offsets[tp.topic]
                                if so and current_pos >= so:
                                    del startup_offsets[tp.topic]

                        metrics['listen'] = listen_status
                        stats = json.dumps(metrics, indent=4, default=str)

                        if self.stats_output:
                            stats_output = Path(self.stats_output)
                            stats_output.parent.mkdir(parents=True, exist_ok=True)

                            with open(stats_output, "w") as f:
                                f.write(stats)

                        self.output.stats = metrics

                    self.output.print()
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
        self.output.close()
