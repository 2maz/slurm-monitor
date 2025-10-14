from argparse import ArgumentParser

from slurm_monitor.cli.base import BaseParser
from slurm_monitor.db.v2.db_testing import (
    start_timescaledb_container,
    create_test_db
)


class TestParser(BaseParser):
    def __init__(self, parser: ArgumentParser):
        super().__init__(parser=parser)

        parser.add_argument("--port",
            type=int,
            default=7777,
            help="Port under which the db shall be accessible"
        )
        parser.add_argument("--user",
            type=str,
            default="test",
            help="Database user"
        )
        parser.add_argument("--password",
            type=str,
            default="test",
            help="Database password"
        )

        parser.add_argument("--image",
            type=str,
            default="timescale/timescaledb:latest-pg17",
            help="Database image"
        )

        parser.add_argument("--container-name","--name",
            type=str,
            default="timescaledb-test",
            help="Name of the container"
        )

        parser.add_argument("--with-stats",
                action="store_true",
                default=False
        )


    def execute(self, args):
        super().execute(args)

        uri = start_timescaledb_container(
                port=args.port,
                user=args.user,
                password=args.password,
                container_name=args.container_name,
                image=args.image,
                stats=True
        )

        create_test_db(uri)
        print(uri)
