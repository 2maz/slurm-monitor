from argparse import ArgumentParser

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
                help=f"Cluster to for which the topics shall be extracted"
        )

        parser.add_argument("--topic",
                nargs="+",
                type=str,
                default=None,
                help="Topic name(s) - if given, cluster-name has no relevance"
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
        

    def execute(self, args):
        super().execute(args)

        app_settings = AppSettings.initialize()

        if args.db_uri is not None:
            app_settings.database.uri = args.db_uri

        if args.use_version == "v1":
            from slurm_monitor.db.v1.data_subscriber import main
            from slurm_monitor.db.v1.db import SlurmMonitorDB

            database = ClusterDB(db_settings=app_settings.database)

            main(host=args.host,
                 port=args.port,
                 database=database,
                 topic=args.topic)
        elif args.use_version == "v2":
            from slurm_monitor.db.v2.data_subscriber import main
            from slurm_monitor.db.v2.db import ClusterDB

            database = ClusterDB(db_settings=app_settings.database)

            # Use asyncio.run to start the event loop and run the main coroutine
            main(host=args.host,
                port=args.port,
                database=database,
                topic=args.topic,
                cluster_name=args.cluster_name,
                verbose=args.verbose,
            )


