from __future__ import annotations

import asyncio
import curses
from enum import Enum
from operator import itemgetter
import logging
from logging.handlers import QueueHandler, TimedRotatingFileHandler
import collections
import datetime as dt
import kafka
from kafka import KafkaConsumer, TopicPartition
import json
from pathlib import Path
from pydantic import BaseModel
import re
import sqlalchemy
import sys
import time
from typing import Iterable, Callable
import traceback as tb
import threading
import warnings

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

class TerminalDisplay:
    stop: bool
    clusters: dict[str, MessageSubscriber.Output]

    rx_fn: Callable[MessageSubscriber.Output]
    rx_thread: threading.Thread

    tx_fn: Callable[[str, MessageSubscriber.Control]]

    log_output: Path | None

    _getch_supported: bool

    def __init__(self, rx_fn: Callable[MessageSubscriber.Output], tx_fn: Callable[[str,MessageSubscriber.Control]], log_output: Path | None = None,
                 log_level: int = logging.INFO):
        self._screen = None

        self.current_cluster_index = 0
        self.clusters = {}

        self.current_tab_index = 0
        self.tabs = [
            "messages",
            "statistics"
        ]

        self.rx_fn = rx_fn
        self.tx_fn = tx_fn

        self.stop = False

        # self.clusters is written by rx_thread (receive) and read by the main
        # thread (show); guard it so a cluster showing up mid-render can't
        # raise "dictionary changed size during iteration" and kill the display
        self._clusters_lock = threading.Lock()

        self.rx_thread = threading.Thread(target=self.receive)

        self.log_output = log_output
        if self.log_output:
            formatter = logging.Formatter(
                fmt=SLURM_MONITOR_LOG_FORMAT,
                datefmt=SLURM_MONITOR_LOG_DATE_FORMAT,
                style=SLURM_MONITOR_LOG_STYLE
            )

            # setup the logging
            root_logger = logging.getLogger()
            root_logger.handlers.clear()

            file_handler = TimedRotatingFileHandler(self.log_output, when='d', interval=3, backupCount=1)
            file_handler.setLevel(logging.getLevelName(log_level))
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            root_logger.addHandler(file_handler)

        self._getch_supported = True

    def receive(self):
        while not self.stop:
            output = self.rx_fn()
            if not output:
                time.sleep(0.1)
            else:
                with self._clusters_lock:
                    self.clusters[output.cluster] = output

    def run(self):
        self.rx_thread.start()
        try:
            while not self.stop:
                self.show()
                time.sleep(0.1)
        finally:
            self.stop = True
            self.close()
            self.rx_thread.join()

    def addstr(self, y, x, text, attr = None):
        screenheight, screenwidth = self._screen.getmaxyx()
        if y >= screenheight:
            return

        writeable_x = screenwidth - x -1
        if writeable_x < 1:
            return

        if attr:
            self._screen.addstr(y, x, text[:writeable_x], attr)
        else:
            self._screen.addstr(y, x, text[:writeable_x])

    def show(self):
        try:
            if not self._screen:
                self._screen = curses.initscr()
                self._screen.clear()
                curses.noecho()
                try:
                    curses.cbreak()
                except Exception:
                    logger.warning("Terminal does not support 'cbreak' - key interactions will be disabled")
                    self._getch_supported = False

                self._screen.nodelay(True)

            self._screen.erase()

            # Snapshot clusters under the lock so the rest of this render sees
            # a single consistent view, instead of racing rx_thread as it adds
            # entries (which could otherwise raise "dictionary changed size
            # during iteration" and take the whole display down).
            with self._clusters_lock:
                clusters_snapshot = dict(self.clusters)

            current_cluster = None
            if clusters_snapshot:
                current_cluster = list(clusters_snapshot.keys())[self.current_cluster_index % len(clusters_snapshot)]

            # header
            screenheight, screenwidth = self._screen.getmaxyx()
            current_time = f"-- CURRENT TIME  {utcnow().isoformat(timespec='seconds')} "
            self.addstr(0, 0, f"{current_time}{'-'*(screenwidth-len(current_time))}")
            self.addstr(1, 0, f">> Status: slurm-monitor listen --cluster-name {current_cluster}")
            self.addstr(2, 0, "   q to quit | l to change log level | t to change tabs (" + ','.join(self.tabs) + ")")
            self.addstr(3, 0, "   c to change the cluster")
            self.addstr(4, 0, " "*screenwidth)
            self.addstr(5, 0, f"    UI attached to {len(clusters_snapshot)} listeners (last seen):")
            y_offset = 6

            for idx, (cluster, output) in enumerate(clusters_snapshot.items(), start=y_offset):
                if output.highlight:
                    self.addstr(idx, 0, f"        {cluster.ljust(25)}: {output.highlight.time}")
                else:
                    self.addstr(idx, 0, f"        {cluster.ljust(25)}: 'unknown'")

                y_offset = idx

            y_offset += 1
            self.addstr(y_offset, 0, f"{'-'*screenwidth}")

            if current_cluster:
                output = clusters_snapshot[current_cluster]
                y_offset +=2
                self.addstr(y_offset, 0, f"Cluster {output.cluster}", curses.A_BOLD)
                y_offset+=1
                if output.highlights:
                    for topic, highlight in sorted(output.highlights.items()):
                        if highlight:
                            self.addstr(y_offset, 0, f"[{topic}] {highlight.to_str()}", curses.A_BOLD)
                        else:
                            self.addstr(y_offset, 0, f"[{topic}] waiting for messages", curses.A_BOLD)
                        y_offset += 1
                else:
                    self.addstr(y_offset, 0, "    waiting for messages    ", curses.A_BOLD)
                    y_offset += 1

                tab_name = self.tabs[self.current_tab_index]
                if hasattr(self, f"tab_{tab_name}"):
                    getattr(self, f"tab_{tab_name}")(output=output, y_offset=y_offset, screenwidth=screenwidth)

            if self._getch_supported:
                key = self._screen.getch()
                if key == ord('q'):
                    self.stop = True
                    self.addstr(0,0, "Received user's request to stop ... ]")
                elif key == ord('c'):
                    if clusters_snapshot:
                        self.current_cluster_index = (self.current_cluster_index + 1) % len(clusters_snapshot)
                elif key == ord('l'):
                    if current_cluster:
                        log_level = output.log_level
                        if log_level == logging.CRITICAL:
                            log_level = logging.DEBUG
                        else:
                            # see https://docs.python.org/3/library/logging.html#logging-levels
                            log_level += 10

                        if self.tx_fn:
                            control = MessageSubscriber.Control(log_level=log_level)
                            self.tx_fn(current_cluster, control)
                elif key == ord('t'):
                    self.current_tab_index = (self.current_tab_index + 1) % len(self.tabs)

            self._screen.refresh()
        except Exception as e:
            logger.error(f"Screen update failed: {e}")
            print(f"Screen update failed: {e}")
            tb.print_tb(e.__traceback__)
            sys.exit(0)


    def tab_messages(self, output: MessageSubscriber.Output, y_offset: int, screenwidth: int):
            # Messages
            self.addstr(y_offset,   0, f"{'-'*screenwidth}")
            self.addstr(y_offset+1, 0, f"| Messages (listener log level: {logging.getLevelName(output.log_level)})")
            self.addstr(y_offset+2, 0, f"{'-'*screenwidth}")

            idx = 0
            y_offset += 4
            for idx, msg in enumerate(list(output.messages)[-20:]):
                self.addstr(idx + y_offset, 0, msg)

            # Nodes
            y_offset += idx + 4
            # sort by time, then by name
            timesorted_messages = sorted([(k,v) for k,v in output.msg_timestamps.items()],key=itemgetter(1,0))

            self.addstr(y_offset,   0, f"{'-'*screenwidth}")
            self.addstr(y_offset+1, 0, "| Nodes")
            self.addstr(y_offset+2, 0, f"{'-'*screenwidth}")

            y_offset += 3
            # place the second column at roughly half the screen width,
            # clamped to [40, 100] so it's neither too narrow to be useful
            # nor pushed off-screen on a normal-width terminal
            column_x = int(min(max(screenwidth / 2, 40), 100))
            self.addstr(y_offset, 0, "Recently seen first", curses.A_BOLD)
            for idx, msg in enumerate(reversed(timesorted_messages[-15:])):
                row = f"{msg[1]} {msg[0]}"
                self.addstr(y_offset + idx + 1, 0, row[:column_x-1])
            self.addstr(y_offset + idx + 2, 0, "    ...")

            self.addstr(y_offset, column_x, "Oldest seen first", curses.A_BOLD)
            for idx, msg in enumerate(timesorted_messages[:15]):
                row = f"{msg[1]} {msg[0]}"
                self.addstr(y_offset + idx + 1, column_x, row[:column_x-1])
            self.addstr(y_offset + idx + 2, column_x, "    ...")

            return y_offset

    def tab_statistics(self, output: MessageSubscriber.Output, y_offset: int, screenwidth: int):
        # Statistics
        self.addstr(y_offset,   0, f"{'-'*screenwidth}")
        self.addstr(y_offset+1, 0, f"| Statistics (update in: {output.next_stats_update:.2f} s)")
        self.addstr(y_offset+2, 0, f"{'-'*screenwidth}")

        if output.stats:
            group_header_y = y_offset + 3
            group_sizes = []
            columns = 3

            column_width = 40 # characters
            for group_idx, (group_name, values) in enumerate(output.stats.items()):
                group_header_x = (group_idx % columns)*column_width

                # when to reset the group_header_y, so to progress to the next 'row' of groups
                if group_sizes and group_idx % columns == 0:
                    group_header_y += max(group_sizes) + 4
                    group_sizes = []

                # Group Header
                self.addstr(group_header_y, group_header_x, group_name, curses.A_BOLD)
                # Properties
                for field_idx, (field_name, field_value) in enumerate(values.items()):
                    if type(field_value) is float:
                        field_value = f"{field_value:.5f}"

                    try:
                        y_offset = group_header_y + 2 + field_idx
                        self.addstr(y_offset, group_header_x, f"{field_name}: {field_value}", curses.A_DIM)
                    except curses.error:
                        pass

                group_sizes.append(len(values))
            return y_offset

    def close(self):
        if self._screen:
            self._screen.clear()
            curses.echo()
            curses.nocbreak()
            curses.endwin()


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

    # one KafkaConsumer + DB connection per topic, each run from its own
    # thread by _consume_topic_with_retry(); _stop_event is the shared
    # shutdown signal, and _output_lock guards _topic_snapshots (the
    # per-topic highlight/stats data that _run()'s aggregator loop merges
    # into the single public `output` below)
    _stop_event: threading.Event
    _output_lock: threading.Lock
    _topic_snapshots: dict[str, dict[str, any]]
    # topics _run() spawned a thread for - lets the aggregator loop list
    # every subscribed topic in `output.highlights`, even one that hasn't
    # processed a message yet
    _topics: list[str]

    # consume_topic() polls up to poll_max_records records, or waits up to
    # poll_wait_in_s, whichever comes first, then writes that whole batch to
    # the database in one go instead of one commit per Kafka record.
    poll_max_records: int
    poll_wait_in_s: float

    state: MessageSubscriber.State
    output: Output

    class State(str, Enum):
        INITIALIZING = 'INITIALIZING'
        RUNNING = 'RUNNING'
        STOPPING = 'STOPPING'
        UNKNOWN = 'UNKNOWN'

    class Highlight(BaseModel):
        state: str
        time: str
        last_processed_topic: str
        consumer_record_offset: int
        latency_in_s: float

        def to_str(self):
            return f"[{self.state}][{self.time}] last processed: topic={self.last_processed_topic} offset={self.consumer_record_offset} latency: {self.latency_in_s:.2f}s"

    class Control(BaseModel):
        log_level: int = 0

    class Output:
        cluster: str
        messages: Iterable[str]
        stats: dict[str, any]
        next_stats_update: int
        highlight: MessageSubscriber.Highlight | None
        # per-topic breakdown of `highlight` - one topic's most recently
        # processed message each, rather than just the single most recent
        # one across all topics. Every subscribed topic has an entry from
        # the start (None until its first message) so a quiet topic still
        # shows up rather than silently being absent.
        highlights: dict[str, MessageSubscriber.Highlight | None]

        log_level: int

        current_tab: str
        tabs: dict[str, Callable]

        max_msg_delay: int
        msg_timestamps: dict[str, dt.datetime]

        def __init__(self, cluster: str = ''):
            self.cluster = cluster
            self.messages = collections.deque(maxlen=100)
            self.stats = {}
            self.highlight = None
            self.highlights = {}

            self.next_stats_update = -1
            self.msg_timestamps = {}

            self.log_level = logger.level

        @classmethod
        def from_dict(self, data: dict[str, any]):
            output = MessageSubscriber.Output()
            output.cluster = data['cluster']
            output.messages = collections.deque(data['messages'])
            output.stats = data['stats']
            if "highlight" in data:
                output.highlight = MessageSubscriber.Highlight(**data['highlight'])
            output.highlights = {
                topic: (MessageSubscriber.Highlight(**highlight) if highlight else None)
                for topic, highlight in data.get('highlights', {}).items()
            }
            output.next_stats_update = data['next_stats_update']
            output.msg_timestamps = data['msg_timestamps']
            output.log_level = data['log_level']

            return output

        def __iter__(self):
            yield "cluster", self.cluster
            yield "messages", list(self.messages)
            yield "stats", self.stats
            if self.highlight:
                yield "highlight", self.highlight.model_dump()
            yield "highlights", {
                topic: (highlight.model_dump() if highlight else None)
                for topic, highlight in self.highlights.items()
            }
            yield "next_stats_update", self.next_stats_update
            yield "msg_timestamps", self.msg_timestamps
            yield "log_level", self.log_level

        def put_nowait(self, record: logging.LogRecord):
            self.messages.append(record.message)

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
            output_fn: Callable[Output] | None = None,
            poll_max_records: int = 100,
            poll_wait_in_s: float = 2,
    ):
        self.host = host
        self.port = port
        self.cluster_name = cluster_name

        self.stats_output = stats_output
        self.stats_interval_in_s = stats_interval_in_s

        self.poll_max_records = poll_max_records
        self.poll_wait_in_s = poll_wait_in_s

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
        self.output = MessageSubscriber.Output(cluster=self.cluster_name)

        self._stop_event = threading.Event()
        self._output_lock = threading.Lock()
        self._topic_snapshots = {}
        self._topics = []

        # function that will receive the latest output
        self.output_fn = output_fn


        # setup the logging
        root_logger = logging.getLogger()
        root_logger.handlers.clear()

        formatter = logging.Formatter(
            fmt=SLURM_MONITOR_LOG_FORMAT,
            datefmt=SLURM_MONITOR_LOG_DATE_FORMAT,
            style=SLURM_MONITOR_LOG_STYLE
        )

        queue_handler = QueueHandler(self.output)
        queue_handler.setLevel(logging.getLevelName(log_level))
        queue_handler.setFormatter(formatter)
        logger.addHandler(queue_handler)
        root_logger.addHandler(queue_handler)

        if self.log_output:
            file_handler = TimedRotatingFileHandler(self.log_output, when='d', interval=3, backupCount=1)
            file_handler.setLevel(logging.getLevelName(log_level))
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            root_logger.addHandler(file_handler)
        else:
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(formatter)
            logger.addHandler(stream_handler)
            root_logger.addHandler(stream_handler)


    @classmethod
    def extract_lookback(cls, lookback: str) -> tuple[str, float]:
        """
            return tuple of topic name and hours
        """
        m = re.match(r"[0-9]+(\.[0-9]+)?$", lookback)
        if m:
            return None, float(lookback)

        m = re.match(r"^(?<topic>[^:]+):(?<hours>[0-9]+(\.[0-9]+)?)$", lookback)
        if m:
            return m.group("topic"), float(m.group("hours"))

        raise ValueError(f"Invalid pattern: {lookback} - could not extract lookback")


    @classmethod
    def extract_lookbacks(cls, lookbacks: list[str]) -> dict[str, float]:
        lookbacks_in_h : dict[sonar.TopicType, float] = LOOKBACK_IN_H_DEFAULTS.copy()

        if not lookbacks:
            return lookbacks_in_h

        for lookback in lookbacks:
            topic_name, lookback_in_h = cls.extract_lookback(lookback)
            if topic_name:
                if not hasattr(sonar.TopicType, topic_name):
                    raise ValueError(f"Invalid topic name: {topic_name} - must be one of {[x.name for x in sonar.TopicType]}")
                topic = getattr(sonar.TopicType, topic_name)
                lookbacks_in_h[topic] = lookback_in_h
            else:
                # setting all the defaults
                for x in lookbacks_in_h:
                    lookbacks_in_h[x] = lookback_in_h

        return lookbacks_in_h

    def receive_and_notify(self):
        if self.output_fn:
            self.output.log_level = logger.handlers[0].level
            command = self.output_fn(self.output)
            if command:
                logger.info(f"Control message - received: {command=}")
                log_level = logging.getLevelName(command.log_level)
                logger.info(f"Changing log level to: {log_level}")
                for handler in logger.handlers:
                    handler.setLevel(log_level)


    def _update_snapshot(self, topic: str, **fields):
        """
        Merge `fields` into this topic's entry of `_topic_snapshots`, under
        `_output_lock`. `_run()`'s aggregator loop reads these snapshots to
        build the single public `output` - this is the only place a
        per-topic consumer thread touches shared state.
        """
        with self._output_lock:
            self._topic_snapshots.setdefault(topic, {}).update(fields)

    async def consume_topic(self,
                topic: str,
                consumer: KafkaConsumer,
                msg_handler: Importer,
                startup_offset: int | None = None,
                upper_bound: int | None = None,
            ):
        """
        Consume a single topic until `_stop_event` is set, its upper bound
        (if any) is reached, or an unrecoverable error occurs.

        Runs as the body of one consumer thread's event loop (see
        `_consume_topic_with_retry`) - `consumer` and `msg_handler` are this
        thread's own, not shared with other topics.
        """
        start_time = dt.datetime.now(dt.timezone.utc)

        if msg_handler is None:
            raise ValueError("MessageHandler must be given")

        ignore_integrity_errors = False
        state = self.State.INITIALIZING
        if startup_offset is None:
            state = self.State.RUNNING
        else:
            ignore_integrity_errors = True

        interval_start_time = dt.datetime.now(dt.timezone.utc)
        while not self._stop_event.is_set():
            # output_fn/control-message round-tripping is centralized in
            # _run()'s aggregator loop (the single thread that owns
            # `output`) rather than done here per-topic-thread
            consumer._fetch_all_topic_metadata()
            if not consumer.assignment():
                logger.info(
                    f"{topic}: no consumer assignment, waiting for partitions to become available"
                )
                time.sleep(self.retry_timeout_in_s)
                continue

            # Poll for up to poll_max_records records, or wait up to
            # poll_wait_in_s - whichever comes first - so the whole batch can
            # be written to the database in one go instead of one commit per
            # Kafka record.
            records_by_partition = consumer.poll(
                timeout_ms=int(self.poll_wait_in_s * 1000),
                max_records=self.poll_max_records
            )
            records = [r for recs in records_by_partition.values() for r in recs]

            hard_stop = False
            if records:
                logger.debug(f"{topic}: consuming msg records - start ({len(records)} polled)")
                batch: list[tuple[dict, bool]] = []
                batch_has_job_message = False

                for consumer_record in records:
                    try:
                        if upper_bound is not None and consumer_record.offset >= upper_bound:
                            logger.info(f"MessageSubscriber.consume_topic: {topic.ljust(25)} -- upper bound reached: {upper_bound}. Stopping.")
                            hard_stop = True
                            break

                        if startup_offset is not None and consumer_record.offset >= startup_offset:
                            startup_offset = None

                        if state == self.State.INITIALIZING and startup_offset is None:
                            logger.info(f"{topic}: startup completed: historic message lookup finished (after {(utcnow() - start_time).total_seconds():.2}s)")
                            state = self.State.RUNNING
                            ignore_integrity_errors = False

                        if self._stop_event.is_set():
                            logger.debug(f"{topic}: consuming: stop requested")
                            break

                        msg = consumer_record.value.decode("UTF-8")
                        if self.verbose:
                            logger.info(f"Message: {msg}")

                        # If a sample arrives there should be no duplicates in the database - an exception is the initialization
                        # where historic records are retrieved
                        # Default: allow to update / merge existing information
                        topic_type = sonar.TopicType.infer(topic)
                        update = topic_type != sonar.TopicType.sample
                        if topic_type == sonar.TopicType.job:
                            batch_has_job_message = True

                        batch.append((json.loads(msg), update))

                        now = utcnow()
                        self._update_snapshot(topic, highlight=MessageSubscriber.Highlight(
                                                        state=state.value,
                                                        time=now.isoformat(timespec='milliseconds'),
                                                        last_processed_topic=topic,
                                                        consumer_record_offset=consumer_record.offset,
                                                        latency_in_s=(now.timestamp() - consumer_record.timestamp/1000.0)
                                                ))
                    except Exception as e:
                        if self.verbose:
                            tb.print_tb(e.__traceback__)

                        logger.warning(f"{topic}: message processing failed: {e}")

                if batch:
                    try:
                        logger.debug(f"{topic}: DB insert: batch of {len(batch)} message(s) {ignore_integrity_errors=} - start")
                        await msg_handler.insert_batch(batch, ignore_integrity_errors=ignore_integrity_errors)
                        logger.debug(f"{topic}: DB insert: batch of {len(batch)} message(s) - completed")

                        # Align cluster information from jobs data once per
                        # batch, after this batch's job rows have actually been
                        # committed - batching already coalesces same-poll job
                        # messages, so there is no need for a separate per-record
                        # timing heuristic here.
                        if batch_has_job_message:
                            logging.info(f"{topic}: auto update - aligning cluster information from jobs data - start")
                            await msg_handler.autoupdate(cluster=self.cluster_name)
                            logging.info(f"{topic}: auto update - aligning cluster information from jobs data - completed")
                    except sqlalchemy.exc.OperationalError as e:
                        logger.warning(
                            f"{topic}: OperationalError of database encountered. For now, assuming it is being (re)started."
                            f"Will sleep for {self.retry_timeout_in_s}s -- details: {e}"
                        )
                        time.sleep(self.retry_timeout_in_s)
                    except Exception as e:
                        if self.verbose:
                            tb.print_tb(e.__traceback__)

                        logger.warning(f"{topic}: batch processing failed: {e}")

            if hard_stop:
                break

            # Refresh this topic's stats snapshot on a fixed cadence
            # regardless of whether this particular poll returned any
            # records - an idle topic should still report its (unchanged)
            # last-seen timestamps rather than never updating them.
            if (dt.datetime.now(dt.timezone.utc) - interval_start_time).total_seconds() > self.stats_interval_in_s:
                interval_start_time = dt.datetime.now(dt.timezone.utc)
                try:
                    msg_timestamps = dict(msg_handler.last_msg_per_node)
                    max_delay = 0
                    if msg_timestamps:
                        max_delay = (interval_start_time - min(msg_timestamps.values())).total_seconds()
                    else:
                        logger.warning(f"{topic}: no messages received - {interval_start_time} s")

                    positions = {}
                    consumer._fetch_all_topic_metadata()
                    for tp in consumer.assignment():
                        current_pos = consumer.position(tp)
                        highwater = consumer.highwater(tp)
                        positions[tp.topic] = { 'current': current_pos, 'highwater': highwater }

                        # Include startup cleanup for a topic that has
                        # received no updates during this interval
                        if startup_offset is not None and current_pos >= startup_offset:
                            startup_offset = None

                    metrics = consumer.metrics()
                    metrics['listen'] = {
                                     'positions': positions,
                                     'stats_interval_in_s': self.stats_interval_in_s,
                                     'interval_start_time': interval_start_time,
                                     'max_delay': max_delay
                                    }

                    self._update_snapshot(topic, msg_timestamps=msg_timestamps, metrics=metrics)
                except Exception as e:
                    logger.warning(f"{topic}: updating stats failed: {e}")

        await msg_handler.autoupdate(cluster=self.cluster_name)

    def _consume_topic_with_retry(self,
                topic: str,
                lower_bound: int | None,
                upper_bound: int | None,
            ):
        """
        Thread target: own a single topic's `KafkaConsumer` and DB
        connection for as long as this `MessageSubscriber` runs, retrying on
        connection failure, until `_stop_event` is set or `consume_topic`
        returns (its upper bound reached, or an unrecoverable error).
        """
        db = None
        if self.database:
            db = self.database.clone()
            msg_handler = DBJsonImporter(db=db)
        else:
            msg_handler = Importer()

        try:
            while not self._stop_event.is_set():
                try:
                    logger.info(f"{topic}: subscribing")
                    # https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html
                    consumer = KafkaConsumer(
                                topic,
                                bootstrap_servers=f"{self.host}:{self.port}",
                                **self.kafka_consumer_options
                                )
                    consumer._fetch_all_topic_metadata()

                    # In particular sysinfo messages are expected to run with
                    # low cadence (every 24\,h). While the default
                    # KafkaConsumer seeks to the end of the partition,
                    # sysinfo messages might not appear for hours. Hence, if
                    # no explicit lower bound was given, look back in
                    # history so already-recorded sysinfo messages are
                    # picked up.
                    startup_offset = None
                    if lower_bound is None:
                        tp = TopicPartition(topic, 0)
                        if consumer.partitions_for_topic(topic):
                            topic_type = sonar.TopicType.infer(topic=topic)
                            timelimit = utcnow() - dt.timedelta(seconds=int(self.lookback_in_h.get(topic_type, LOOKBACK_IN_H_DEFAULT)*3600))
                            logger.info(f"{topic=}: search offset for {timelimit=}")
                            offset_and_timestamp = consumer.offsets_for_times({ tp: int(timelimit.timestamp()*1000) })[tp]
                            if offset_and_timestamp:
                                offset = offset_and_timestamp.offset
                                timestamp_in_s = offset_and_timestamp.timestamp / 1000
                                logger.info(f"{topic=}: found {offset=} for {dt.datetime.fromtimestamp(timestamp_in_s)}")
                                lower_bound = offset
                                startup_offset = consumer.end_offsets([tp])[tp]

                    if lower_bound is not None:
                        logger.info(f"{topic=}: seek to {lower_bound}")
                        consumer.seek(TopicPartition(topic, 0), lower_bound)

                    asyncio.run(self.consume_topic(topic, consumer, msg_handler,
                                  startup_offset=startup_offset,
                                  upper_bound=upper_bound))
                    return
                except kafka.errors.NoBrokersAvailable as e:
                    msg = f"{topic}: no brokers available using bootstrap_servers: {self.host}:{self.port} retrying in {self.retry_timeout_in_s}s (check {self.log_output}) - {e}"
                    logger.warning(msg)
                    warnings.warn(msg)
                    time.sleep(self.retry_timeout_in_s)
                except TimeoutError:
                    raise
                except Exception as e:
                    msg = f"{topic}: connection failed - retrying in {self.retry_timeout_in_s}s (see {self.log_output}) - {e}"
                    logger.warning(msg)
                    warnings.warn(msg)
                    time.sleep(self.retry_timeout_in_s)
        finally:
            if db:
                asyncio.run(db.dispose())

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

    def request_stop(self):
        """Signal every topic thread (and `_run()`'s aggregator loop) to stop."""
        self.state = self.State.STOPPING
        self._stop_event.set()

    def _merge_output(self, last_stats_output_time: dt.datetime) -> dt.datetime:
        """
        Merge each topic's latest snapshot (as pushed by its consumer
        thread via `_update_snapshot`) into the single public `output` -
        the shape `listen.py`/`TerminalDisplay` and the stats JSON file
        expect, unchanged by this being backed by several topic threads now.

        Args:
            last_stats_output_time: when `output.stats` (and the stats
                file, if configured) were last refreshed.

        Returns:
            The (possibly updated) `last_stats_output_time` for the next call.
        """
        with self._output_lock:
            snapshots = {topic: dict(fields) for topic, fields in self._topic_snapshots.items()}

        # every subscribed topic gets an entry - None until its first
        # message - so a quiet topic still shows up rather than silently
        # being absent
        highlights: dict[str, MessageSubscriber.Highlight | None] = dict.fromkeys(self._topics)
        for topic, fields in snapshots.items():
            if "highlight" in fields:
                highlights[topic] = fields["highlight"]
        self.output.highlights = highlights

        actual_highlights = [h for h in highlights.values() if h is not None]
        if actual_highlights:
            self.output.highlight = max(actual_highlights, key=lambda h: h.time)

        msg_timestamps: dict[str, dt.datetime] = {}
        for fields in snapshots.values():
            msg_timestamps.update(fields.get("msg_timestamps", {}))
        if msg_timestamps:
            self.output.msg_timestamps = msg_timestamps

        now = dt.datetime.now(dt.timezone.utc)
        self.output.next_stats_update = self.stats_interval_in_s - (now - last_stats_output_time).total_seconds()
        if (now - last_stats_output_time).total_seconds() <= self.stats_interval_in_s:
            return last_stats_output_time

        stats = {topic: fields["metrics"] for topic, fields in snapshots.items() if "metrics" in fields}
        if stats:
            self.output.stats = stats
            if self.stats_output:
                try:
                    stats_output = Path(self.stats_output)
                    stats_output.parent.mkdir(parents=True, exist_ok=True)
                    with open(stats_output, "w") as f:
                        f.write(json.dumps(stats, indent=4, default=str))
                except Exception as e:
                    logger.warning(f"Writing stats file failed: {e}")

        return now

    def run(self):
        try:
            asyncio.run(self._run())
        except KeyboardInterrupt:
            print("Keyboard interrupt received - stopping")
            self.request_stop()

    async def _run(self):
        """
        Spawn one consumer thread per topic, each with its own KafkaConsumer
        and DB connection, and run this call's own thread as an aggregator:
        periodically merging every topic's latest status into the single
        public `output`, and forwarding/receiving control messages via
        `receive_and_notify()`.

        Note that a topic can be defined with a lower bound and and upper bound offset, e.g., as "<topic_name>:<lb-offset>-<ub-offset>.
            - when an lower bound offset is defined: start the consumption of messages for the related topic at this message offset
            - when an upper bound offset is defined: end the consumption of messages for the related topic, when a (topic) message with an offset equal or larger than this bound is encountered.
        """

        if self.strict_mode:
            TableBase.__extra_values__ = 'forbid'

        if not self.database:
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
        self._stop_event.clear()
        self._topics = topics

        logger.info(f"Subscribing to topics: {topics}")
        threads = [
            threading.Thread(
                target=self._consume_topic_with_retry,
                args=(topic, topic_lb.get(topic), topic_ub.get(topic)),
                name=f"consume-{topic}",
                daemon=True,
            )
            for topic in topics
        ]
        for thread in threads:
            thread.start()

        self.state = self.State.RUNNING
        last_stats_output_time = dt.datetime.now(dt.timezone.utc)
        try:
            while not self._stop_event.is_set() and any(t.is_alive() for t in threads):
                last_stats_output_time = self._merge_output(last_stats_output_time)
                self.receive_and_notify()
                await asyncio.sleep(self.poll_wait_in_s)
        finally:
            self.request_stop()
            for thread in threads:
                thread.join()

        logger.info("All tasks gracefully stopped")
