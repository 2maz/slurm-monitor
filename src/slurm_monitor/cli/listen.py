from argparse import ArgumentParser
import json
import re
import sqlalchemy
from sqlalchemy import inspect
import sys
import time
import zmq

from slurm_monitor.cli.base import BaseParser
from slurm_monitor.app_settings import (
    AppSettings,
)
from slurm_monitor.db.v2.message_subscriber import MessageSubscriber, TerminalDisplay

import logging

logger = logging.getLogger(__name__)

class ListenParser(BaseParser):
    socket: zmq.Socket

    ui_host: str | None
    ui_port: int | None

    cluster_name: str | None

    message_tx_count: int
    message_rx_count: int

    warn_missing_ui: bool

    retry_timeout_in_s: int

    def __init__(self, parser: ArgumentParser):
        super().__init__(parser=parser)

        self.message_tx_count = 0
        self.message_rx_count = 0
        self.warn_missing_ui = True

        app_settings = AppSettings.get_instance()
        parser.description = "listen - The listener component establishes a link between a kafka broker and a database. The (sonar) messages received by the kafka broker will be stored in the database. A listener can be monitored with the listen-ui component"

        parser.add_argument("--host", type=str,
                            default=app_settings.listen.kafka.host,
                            required=(app_settings.listen.kafka.host is None),
                            help=f"Set the kafka broker host, default is {app_settings.listen.kafka.host}"
        )

        parser.add_argument("--db-uri",
                            type=str,
                            default=app_settings.database.uri,
                            help=f"Set the database connection sqlite:////tmp/sqlite.db or timescaledb://slurmuser:test@localhost:7000/ex3cluster"
                                f" default is {app_settings.database.uri}"
        )

        parser.add_argument("--port", type=int,
                            default=app_settings.listen.kafka.port,
                            help=f"Set the kafka brokers port to connect to, default is {app_settings.listen.kafka.port}"
        )

        parser.add_argument("--cluster-name",
                type=str,
                default=app_settings.listen.cluster,
                help=f"Cluster for which the topics shall be extracted, default is {app_settings.listen.cluster}",
                required=(app_settings.listen.cluster is None),
        )

        parser.add_argument("--topic",
                nargs="+",
                type=str,
                default=None,
                help="Topic name(s) - if given, cluster-name has no relevance " \
                        "can be used with lower and upper offset bounds <topic-name>:<lb-offset-<ub-offset>" \
                        " after reached the upper bound, processing will be stopped for the topic"
        )

        parser.add_argument("--use-version",
                type=str,
                default="v2",
                help="Use this API and DB version"
        )

        parser.add_argument("--use-strict-mode",
                action="store_true",
                default=False,
                help="When receiving message, insert content only for messsages that contain only values "\
                     "present in the table schema. This means, message format and table schema need to be in sync, since no extra / new fields are allowed"
        )

        parser.add_argument("--lookback",
                nargs="+",
                type=str,
                default=app_settings.listen.lookback,
                help="Define the lookback timeframe in hours, e.g., 1 or 0.1, for all and/or specific topics: "
                      "'--lookback 0.5 cluster:1.5' will apply 0.5 to all topics, but cluster, "
                      f"which will use a lookback time of 1.5 hours, default is {app_settings.listen.lookback}"
        )

        parser.add_argument("--stats-output",
                type=str,
                default=None,
                help="Output file for the listen stats - default: slurm-monitor.listen.<cluster-name>.stats.json",
        )

        parser.add_argument("--stats-interval",
                type=int,
                default=app_settings.listen.stats.interval,
                help=f"Interval in seconds for generating stats output, default is {app_settings.listen.stats.interval}"
        )

        parser.add_argument("--log-output",
                type=str,
                default=None,
                help="Output file for the log - default: slurm-monitor.listen.<cluster-name>.log, disable logging to file by specifying 'none'",
        )

        parser.add_argument("--ui-host",
                            type=str,
                            default=app_settings.listen.ui.host,
                            help=f"Set the ui host, default is {app_settings.listen.ui.host}")

        parser.add_argument("--ui-port",
                            type=int,
                            default=app_settings.listen.ui.port,
                            help=f"Set the ui port, default is {app_settings.listen.ui.port}")

        parser.add_argument("--retry-timeout-in-s",
                            type=int,
                            default=app_settings.listen.retry,
                            help="Set retry timeout, when database or partition are not available")


    def publish_status(self, output: MessageSubscriber.Output) -> MessageSubscriber.Control:
        """
        zmq Dealer/Router pattern in use: this is the 'Dealer' publishing / and receiving instructions
        """
        cluster = self.cluster_name
        try:
            logger.debug(f"Listen.publish_status (to listen-ui) - start (from {cluster}) (hwm: {self.socket.get_hwm()})")
            data = dict(output)
            self.socket.send_json(data, default=str, flags=zmq.NOBLOCK)
            self.message_tx_count += 1
            logger.debug(f"Listen.publish_status (to listen-ui): complete (message_tx_count={self.message_tx_count} {cluster=})")
            if not self.warn_missing_ui:
                logger.warning("Listen.publish_status (to listen-ui): listen ui connected")
                self.warn_missing_ui = True
        except zmq.error.Again:
            if self.warn_missing_ui:
                logger.warning(f"Listen.publish_status: Socket is full (highwatermark: {self.socket.get_hwm()} reached, message could not be forwarded to listen-ui. (no further warning will be shown until ui is up and receiving")
                self.warn_missing_ui = False
            else:
                logger.debug(f"Listen.publish_status: Socket is full (highwatermark: {self.socket.get_hwm()} reached, message could not be forwarded to listen-ui.")
            # If ui is not available not need to check for received control commands
            return None
        except zmq.error.ZMQError as e:
            logger.debug(f"Listen.publish_status: zmq error (from {cluster}) -- {e}")
            pass

        try:
            logger.debug(f"Listen.publish_status (check control cmds from listen-ui) recv_multipart: start (from {cluster})")
            empty, json_bytes = self.socket.recv_multipart(zmq.NOBLOCK)
            self.message_rx_count += 1
            logger.debug(f"Listen.publish_status (check control cmds from listen-ui) recv_multipart: complete (message_rx_count={self.message_rx_count} {cluster=})")
            control = json.loads(json_bytes.decode("UTF-8"))
            return MessageSubscriber.Control(**control)
        except zmq.error.ZMQError:
            logger.debug(f"Listen.recv_multipart: error / nothing to receive (from {cluster})")
            return None
        except json.decoder.JSONDecodeError:
            logger.warning(f"Listen.recv_multipart: json decoding error for {json_bytes=} (from {cluster})")
            return None


    def execute(self, args):
        super().execute(args)

        app_settings = AppSettings.get_instance()

        if args.cluster_name is None:
            raise ValueError("Missing cluster name - please specify with --cluster-name")

        if args.db_uri is not None:
            app_settings.database.uri = args.db_uri

        # Listener should only operate on an already initialized database
        app_settings.database.create_missing = False

        if args.use_version == "v1":
            from slurm_monitor.db.v1.data_subscriber import main
            from slurm_monitor.db.v1.db import SlurmMonitorDB

            database = SlurmMonitorDB(db_settings=app_settings.database)

            main(host=args.host,
                 port=args.port,
                 database=database,
                 topic=args.topic)
        elif args.use_version == "v2":
            from slurm_monitor.db.v2.db import ClusterDB

            # Ensure commandline overrides .env file settings
            if args.cluster_name:
                app_settings.listen.cluster = args.cluster_name
                self.cluster_name = args.cluster_name

            if args.port:
                app_settings.listen.kafka.port = args.port

            if args.host:
                app_settings.listen.kafka.host = args.host

            lookback_in_h = MessageSubscriber.extract_lookbacks(args.lookback)

            database = None
            if args.db_uri is not None:
                database = ClusterDB(db_settings=app_settings.database)

                inspector = None
                while inspector is None:
                    try:
                        inspector = inspect(database.engine)
                    except sqlalchemy.exc.OperationalError as e:
                        if re.search("refused", str(e)) is not None:
                            logger.warning("Connection refused - please verify connection settings.")
                            sys.exit(10)
                        elif re.search("is starting up", str(e)) is not None:
                            logger.warning(f"Database is starting up - retrying in {args.retry_timeout_in_s}s -- {e}")
                        elif re.search("server closed the connection unexpectedly", str(e)) is not None:
                            logger.warning(f"Database closed connection - retrying in {args.retry_timeout_in_s}s -- {e}")
                        else:
                            raise

                        time.sleep(args.retry_timeout_in_s)

                if not inspector.get_table_names():
                    raise RuntimeError("Listener is trying to connect to an uninitialized database."
                                       f" Call 'slurm-monitor db --init --db-uri {app_settings.database.uri}' for the database first")

                suggested_lookback = database.suggest_lookback(args.cluster_name)
                logger.info("Adapting lookback time based on db information")
                for topic, suggested in suggested_lookback.items():
                    lookback_in_h[topic] = min(lookback_in_h[topic], suggested)
            else:
                logger.info("Running in listen mode")

            print("Connecting with")
            print(f"    kafka bootstrap server: {app_settings.listen.kafka.host}:{app_settings.listen.kafka.port}")
            print(f"    listen-ui: {args.ui_host}:{args.ui_port}")
            print("    lookbacks: ")
            for x,y in lookback_in_h.items():
                print(f"        {x.rjust(10)}: {str(y).rjust(4)}h")

            stats_output = args.stats_output
            if stats_output is None:
                stats_output = "slurm-monitor.listen.stats.json"
                if args.cluster_name:
                    stats_output = f"slurm-monitor.listen.{args.cluster_name}.stats.json"

            log_output = args.log_output
            # if log_output is None (the default), we set the default
            # log file name
            if log_output is None:
                log_output = "slurm-monitor.listen.log"
                if args.cluster_name:
                    log_output = f"slurm-monitor.listen.{args.cluster_name}.log"
            # to explicitely disable log_output, user needs to set it to 'none'
            elif log_output.lower() == 'none':
                log_output = None
            else:
                # user has specified a 'custom' filename for logging, so use it
                pass
            print(f"Logging to: {log_output}")

            context = zmq.Context()
            self.socket = context.socket(zmq.DEALER)

            # default is an infinite wait (-1)
            self.socket.RCVTIMEO = 0
            # default is an infinite wait (-1)
            self.socket.SNDTIMEO = 0
            # Setting the high-water mark to keep only one message
            self.socket.SNDHWM = 1
            # immediately discard messages in memory if socket is closed
            self.socket.LINGER = 0

            if args.ui_host and args.ui_port:
                self.socket.setsockopt_string(zmq.IDENTITY, args.cluster_name)
                self.socket.connect(f"tcp://{args.ui_host}:{args.ui_port}")


            subscriber = MessageSubscriber(
                    host=args.host,
                    port=args.port,
                    database=database,
                    topics=args.topic,
                    cluster_name=args.cluster_name,
                    verbose=args.verbose,
                    strict_mode=args.use_strict_mode,
                    lookback_in_h=lookback_in_h,
                    stats_output=stats_output,
                    stats_interval_in_s=args.stats_interval,
                    log_output=log_output,
                    log_level=args.log_level,
                    output_fn=self.publish_status,
                    retry_timeout_in_s=args.retry_timeout_in_s
                )
            subscriber.run()

class ListenUiParser(BaseParser):
    socket: zmq.Socket

    ui_host: str | None
    ui_port: int | None

    message_tx_count: int
    message_rx_count: int

    def __init__(self, parser: ArgumentParser):
        super().__init__(parser=parser)

        self.message_tx_count = 0
        self.message_rx_count = 0

        app_settings = AppSettings.get_instance()

        parser.add_argument("--cluster-name",
                nargs="+",
                type=str,
                default=app_settings.listen.cluster,
                help="Cluster(s) for which the ui runs (used only for naming the log file accordingly)"
        )

        parser.add_argument("--ui-host",
                            type=str,
                            default="*",
                            help="Set the ui host, default is *")

        parser.add_argument("--ui-port",
                            type=int,
                            default=app_settings.listen.ui.port,
                            help=f"Set the ui port, default is {app_settings.listen.ui.port}")

        parser.add_argument("--log-output",
                type=str,
                default=None,
                help="Output file for the log - default: slurm-monitor.listen-ui.<cluster-name>.log, disable logging to file by specifying 'none'",
        )

    def execute(self, args):
        super().execute(args)

        context = zmq.Context()
        self.socket = context.socket(zmq.ROUTER)

        # http://api.zeromq.org/3-0:zmq-setsockopt
        # default is an infinite wait (-1)
        self.socket.RCVTIMEO = 0
        # default is an infinite wait (-1)
        self.socket.SNDTIMEO = 0
        # Setting the high-water mark to keep only one message
        self.socket.SNDHWM = 1

        # immediately discard messages in memory if socket is closed
        self.socket.LINGER = 0

        if args.ui_host and args.ui_port:
            self.socket.bind(f"tcp://{args.ui_host}:{args.ui_port}")

        log_output = args.log_output
        # if log_output is None (the default), we set the default
        # log file name
        if log_output is None:
            log_output = "slurm-monitor.listen-ui.log"
            if args.cluster_name:
                log_output = f"slurm-monitor.listen-ui.{'+'.join(args.cluster_name)}.log"
        # to explicitely disable log_output, user needs to set it to 'none'
        elif log_output.lower() == 'none':
            log_output = None
        else:
            # user has specified a 'custom' filename for logging, so use it
            pass

        def update():
            global message_rx_count
            """
            zmq DEALER / ROUTER pattern: receiving a multipart message from dealer here
            """
            try:
                logger.debug("ListenUiParser.recv_multipart: start")
                dealer_id, json_content = self.socket.recv_multipart(zmq.NOBLOCK)
                self.message_rx_count += 1
                logger.info(f"ListenUiParser.recv_multipart: complete (message_rx_count={self.message_rx_count} {dealer_id=})")
                message = json.loads(json_content)
                return MessageSubscriber.Output.from_dict(message)
            except zmq.error.ZMQError:
                logger.debug("ListenUiParser.recv_multipart: error / nothing to receive")
                return None
            except json.decoder.JSONDecodeError:
                logger.warning(f"Failed to decode json content: message_rx_count={self.message_rx_count} {dealer_id=} {json_content=}")
                return None

        def send(dealer_id: str, control: MessageSubscriber.Control):
            json_bytes = json.dumps(control.model_dump()).encode("UTF-8")
            try:
                logger.debug(f"ListenUiParser.send_multipart: start (to {dealer_id=})")
                self.socket.send_multipart([dealer_id.encode("UTF-8"), b"", json_bytes])
                self.message_tx_count +=1
                logger.info(f"ListenUiParser.send_multipart: complete (message_tx_count={self.message_tx_count} {dealer_id=} {json_bytes=})")
            except zmq.error.Again:
                logger.warning(f"Socket is full (highwatermark: {self.socket.get_hwm()} reached): message could not be forwarded to listener {dealer_id=}")
            except zmq.error.ZMQError:
                logger.warning(f"ListenUiParser.send_multipart: Error sending {dealer_id=} {json_bytes=}")


        display = TerminalDisplay(rx_fn=update, tx_fn=send, log_output=log_output, log_level=args.log_level)
        display.run()
