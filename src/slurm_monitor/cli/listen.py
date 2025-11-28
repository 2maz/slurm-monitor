from argparse import ArgumentParser
from sqlalchemy import inspect

from slurm_monitor.cli.base import BaseParser
from slurm_monitor.app_settings import AppSettings

class ListenParser(BaseParser):
    def __init__(self, parser: ArgumentParser):
        super().__init__(parser=parser)

        parser.add_argument("--host", type=str, default=None, required=True)
        parser.add_argument("--db-uri", type=str, default=None, help="sqlite:////tmp/sqlite.db or timescaledb://slurmuser:test@localhost:7000/ex3cluster")
        parser.add_argument("--port", type=int, default=9099)

        parser.add_argument("--cluster-name",
                type=str,
                help="Cluster to for which the topics shall be extracted"
        )

        parser.add_argument("--topic",
                nargs="+",
                type=str,
                default=None,
                help="Topic name(s) - if given, cluster-name has no relevance " \
                        "can be used with lower and upper offset bounds <topic-name>:<lb-offset-<ub-offset>" \
                        " after reached the upper bound, processing will be stopped for the topic"
        )

        parser.add_argument("--verbose",
                action="store_true",
                default=False,
                help="Print messages"
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
            from slurm_monitor.db.v2.message_subscriber import MessageSubscriber
            from slurm_monitor.db.v2.db import ClusterDB

            database = ClusterDB(db_settings=app_settings.database)
            inspector = inspect(database.engine)
            if not inspector.get_table_names():
                raise RuntimeError("Listener is trying to connect to an uninitialized database."
                                   f" Call 'slurm-monitor db --init --db-uri {app_settings.database.uri}' for the database first")

            subscriber = MessageSubscriber(
                    host=args.host,
                    port=args.port,
                    database=database,
                    topics=args.topic,
                    cluster_name=args.cluster_name,
                    verbose=args.verbose,
                    strict_mode=args.use_strict_mode
                )
            subscriber.run()
