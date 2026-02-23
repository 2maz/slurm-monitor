from argparse import ArgumentParser
import json
from sqlalchemy import inspect
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

    def __init__(self, parser: ArgumentParser):
        super().__init__(parser=parser)

        app_settings = AppSettings.get_instance()

        parser.add_argument(
            "--host",
            type=str,
            default=app_settings.listen.kafka.host,
            required=(app_settings.listen.kafka.host is None),
            help=f"Set the kafka broker host, default is {app_settings.listen.kafka.host}",
        )

        parser.add_argument(
            "--db-uri",
            type=str,
            default=app_settings.database.uri,
            help=f"Set the database connection sqlite:////tmp/sqlite.db or timescaledb://slurmuser:test@localhost:7000/ex3cluster"
            f" default is {app_settings.database.uri}",
        )

        parser.add_argument(
            "--port",
            type=int,
            default=app_settings.listen.kafka.port,
            help=f"Set the kafka brokers port to connect to, default is {app_settings.listen.kafka.port}",
        )

        parser.add_argument(
            "--cluster-name",
            type=str,
            default=app_settings.listen.cluster,
            help=f"Cluster for which the topics shall be extracted, default is {app_settings.listen.cluster}",
            required=(app_settings.listen.cluster is None),
        )

        parser.add_argument(
            "--topic",
            nargs="+",
            type=str,
            default=None,
            help="Topic name(s) - if given, cluster-name has no relevance "
            "can be used with lower and upper offset bounds <topic-name>:<lb-offset-<ub-offset>"
            " after reached the upper bound, processing will be stopped for the topic",
        )

        parser.add_argument(
            "--use-version", type=str, default="v2", help="Use this API and DB version"
        )

        parser.add_argument(
            "--use-strict-mode",
            action="store_true",
            default=False,
            help="When receiving message, insert content only for messsages that contain only values "
            "present in the table schema. This means, message format and table schema need to be in sync, since no extra / new fields are allowed",
        )

        parser.add_argument(
            "--lookback",
            nargs="+",
            type=str,
            default=app_settings.listen.lookback,
            help="Define the lookback timeframe in hours, e.g., 1 or 0.1, for all and/or specific topics: "
            "'--lookback 0.5 cluster:1.5' will apply 0.5 to all topics, but cluster, "
            f"which will use a lookback time of 1.5 hours, default is {app_settings.listen.lookback}",
        )

        parser.add_argument(
            "--stats-output",
            type=str,
            default=None,
            help="Output file for the listen stats - default: slurm-monitor-listen.<cluster-name>.stats.json",
        )

        parser.add_argument(
            "--stats-interval",
            type=int,
            default=app_settings.listen.stats.interval,
            help=f"Interval in seconds for generating stats output, default is {app_settings.listen.stats.interval}",
        )

        parser.add_argument(
            "--log-output",
            type=str,
            default=None,
            help="Output file for the log - default: slurm-monitor-listen.<cluster-name>.log",
        )

        parser.add_argument(
            "--ui-host",
            type=str,
            default=app_settings.listen.ui.host,
            help=f"Set the ui host, default is {app_settings.listen.ui.host}",
        )

        parser.add_argument(
            "--ui-port",
            type=int,
            default=app_settings.listen.ui.port,
            help=f"Set the ui port, default is {app_settings.listen.ui.port}",
        )

    def publish_status(
        self, output: MessageSubscriber.Output
    ) -> MessageSubscriber.Control:
        """
        zmq Dealer/Router pattern in use: this is the 'Dealer' publishing / and receiving instructions
        """
        self.socket.send_json(dict(output), default=str)
        try:
            empty, json_bytes = self.socket.recv_multipart(zmq.NOBLOCK)
            control = json.loads(json_bytes.decode("UTF-8"))
            return MessageSubscriber.Control(**control)
        except zmq.error.ZMQError:
            return None

    def execute(self, args):
        super().execute(args)

        app_settings = AppSettings.get_instance()

        if args.cluster_name is None:
            raise ValueError(
                "Missing cluster name - please specify with --cluster-name"
            )

        if args.db_uri is not None:
            app_settings.database.uri = args.db_uri

        # Listener should only operate on an already initialized database
        app_settings.database.create_missing = False

        if args.use_version == "v1":
            from slurm_monitor.db.v1.data_subscriber import main
            from slurm_monitor.db.v1.db import SlurmMonitorDB

            database = SlurmMonitorDB(db_settings=app_settings.database)

            main(host=args.host, port=args.port, database=database, topic=args.topic)
        elif args.use_version == "v2":
            from slurm_monitor.db.v2.db import ClusterDB

            lookback_in_h = MessageSubscriber.extract_lookbacks(args.lookback)

            database = None
            if args.db_uri is not None:
                database = ClusterDB(db_settings=app_settings.database)
                inspector = inspect(database.engine)
                if not inspector.get_table_names():
                    raise RuntimeError(
                        "Listener is trying to connect to an uninitialized database."
                        f" Call 'slurm-monitor db --init --db-uri {app_settings.database.uri}' for the database first"
                    )

                suggested_lookback = database.suggest_lookback(args.cluster_name)
                logger.info("Adapting lookback time based on db information")
                for topic, suggested in suggested_lookback.items():
                    lookback_in_h[topic] = min(lookback_in_h[topic], suggested)
            else:
                logger.info("Running in listen mode")

            print("Using lookbacks: ")
            for x, y in lookback_in_h.items():
                print(f"    {x.rjust(10)}: {str(y).rjust(4)}h")

            stats_output = args.stats_output
            if stats_output is None:
                stats_output = "slurm-monitor-listen.stats.json"
                if args.cluster_name:
                    stats_output = (
                        f"slurm-monitor-listen.{args.cluster_name}.stats.json"
                    )

            log_output = args.log_output
            if log_output is None:
                log_output = "slurm-monitor-listen.log"
                if args.cluster_name:
                    log_output = f"slurm-monitor-listen.{args.cluster_name}.log"

            context = zmq.Context()
            self.socket = context.socket(zmq.DEALER)

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
            )
            subscriber.run()


class ListenUiParser(BaseParser):
    socket: zmq.Socket

    ui_host: str | None
    ui_port: int | None

    def __init__(self, parser: ArgumentParser):
        super().__init__(parser=parser)

        app_settings = AppSettings.get_instance()

        parser.add_argument(
            "--cluster-name",
            nargs="+",
            type=str,
            default=app_settings.listen.cluster,
            help=f"Cluster(s) for which the topics shall be extracted, default is {app_settings.listen.cluster}",
        )

        parser.add_argument(
            "--ui-host", type=str, default="*", help="Set the ui host, default is *"
        )

        parser.add_argument(
            "--ui-port",
            type=int,
            default=app_settings.listen.ui.port,
            help=f"Set the ui port, default is {app_settings.listen.ui.port}",
        )

        parser.add_argument(
            "--log-output",
            type=str,
            default=None,
            help="Output file for the log - default: slurm-monitor-listen-ui.<cluster-name>.log",
        )

    def execute(self, args):
        super().execute(args)

        context = zmq.Context()
        self.socket = context.socket(zmq.ROUTER)

        if args.ui_host and args.ui_port:
            self.socket.bind(f"tcp://{args.ui_host}:{args.ui_port}")

        log_output = args.log_output
        if log_output is None:
            log_output = "slurm-monitor-listen-ui.log"
            if args.cluster_name:
                log_output = f"slurm-monitor-listen-ui.{args.cluster_name}.log"

        def update():
            """
            zmq DEALER / ROUTER pattern: receiving a multipart message from dealer here
            """
            try:
                dealer_id, json_content = self.socket.recv_multipart(zmq.NOBLOCK)
                message = json.loads(json_content)
                return MessageSubscriber.Output.from_dict(message)
            except zmq.error.ZMQError:
                return None

        def send(dealer_id: str, control: MessageSubscriber.Control):
            json_bytes = json.dumps(control.model_dump()).encode("UTF-8")
            self.socket.send_multipart([dealer_id.encode("UTF-8"), b"", json_bytes])

        display = TerminalDisplay(rx_fn=update, tx_fn=send, log_output=log_output)
        display.run()
