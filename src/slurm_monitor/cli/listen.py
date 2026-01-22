from argparse import ArgumentParser
from sqlalchemy import inspect

from slurm_monitor.cli.base import BaseParser
from slurm_monitor.app_settings import AppSettings

import logging

logger = logging.getLogger(__name__)

class ListenParser(BaseParser):
    def __init__(self, parser: ArgumentParser):
        super().__init__(parser=parser)

        parser.add_argument("--host", type=str, default=None, required=True)
        parser.add_argument("--db-uri", type=str, default=None, help="sqlite:////tmp/sqlite.db or timescaledb://slurmuser:test@localhost:7000/ex3cluster")
        parser.add_argument("--port", type=int, default=9099)

        parser.add_argument("--cluster-name",
                type=str,
                help="Cluster for which the topics shall be extracted"
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
                default=None,
                help="Define the lookback timeframe in hours, e.g., 1 or 0.1, for all and/or specific topics: "
                      "'--lookback 0.5 cluster:1.5' will apply 0.5 to all topics, but cluster, "
                      "which will use a lookback time of 1.5 hours"
        )

        parser.add_argument("--stats-output",
                type=str,
                default=None,
                help="Output file for the listen stats - default: slurm-monitor-listen.<cluster-name>.stats.json",
        )

        parser.add_argument("--stats-interval",
                type=int,
                default=30,
                help="Interval in seconds for generating stats output"
        )


    def execute(self, args):
        super().execute(args)

        app_settings = AppSettings.initialize()

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
            from slurm_monitor.db.v2.message_subscriber import (
                    MessageSubscriber
            )
            from slurm_monitor.db.v2.db import ClusterDB

            lookback_in_h = MessageSubscriber.extract_lookbacks(args.lookback)
            print("Using lookbacks: ")
            for x,y in lookback_in_h.items():
                print(f"    {x.rjust(10)}: {str(y).rjust(4)}h")

            database = None
            if args.db_uri is not None:
                database = ClusterDB(db_settings=app_settings.database)
                inspector = inspect(database.engine)
                if not inspector.get_table_names():
                    raise RuntimeError("Listener is trying to connect to an uninitialized database."
                                       f" Call 'slurm-monitor db --init --db-uri {app_settings.database.uri}' for the database first")
            else:
                logger.info("Running in listen mode")

            stats_output = args.stats_output
            if stats_output is None:
                stats_output = "slurm-monitor-listen.stats.json"
                if args.cluster_name:
                    stats_output = f"slurm-monitor-listen.{args.cluster_name}.stats.json"

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
                    stats_interval_in_s=args.stats_interval
                )
            subscriber.run()
