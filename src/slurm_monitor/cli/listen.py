from argparse import ArgumentParser

from slurm_monitor.cli.base import BaseParser
from slurm_monitor.db.v1.data_subscriber import main

from slurm_monitor.db.v1.data_publisher import KAFKA_NODE_STATUS_TOPIC

from slurm_monitor.db.v1.db import SlurmMonitorDB
from slurm_monitor.app_settings import AppSettings

class ListenParser(BaseParser):
    def __init__(self, parser: ArgumentParser):
        super().__init__(parser=parser)

        parser.add_argument("--host", type=str, default=None, required=True)
        parser.add_argument("--db-uri", type=str, default=None, help="sqlite:////tmp/sqlite.db or timescaledb://slurmuser:test@localhost:10100/ex3cluster")
        parser.add_argument("--port", type=int, default=10092)

        parser.add_argument("--topic",
                type=str,
                default=KAFKA_NODE_STATUS_TOPIC,
                help=f"Topic which is subscribed -- default {KAFKA_NODE_STATUS_TOPIC}"
        )

    def execute(self, args):
        super().execute(args)

        app_settings = AppSettings.initialize()

        if args.db_uri is not None:
            app_settings.database.uri = args.db_uri

        database = SlurmMonitorDB(db_settings=app_settings.database)

        # Use asyncio.run to start the event loop and run the main coroutine
        main(host=args.host,
            port=args.port,
            database=database,
            topic=args.topic)
