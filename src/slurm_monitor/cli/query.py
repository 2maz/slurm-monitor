from argparse import ArgumentParser

from slurm_monitor.cli.base import BaseParser
from slurm_monitor.db.v1.db import SlurmMonitorDB
from slurm_monitor.db.v1.query import QueryMaker
from slurm_monitor.app_settings import AppSettings


class QueryParser(BaseParser):
    def __init__(self, parser: ArgumentParser):
        super().__init__(parser=parser)

        parser.add_argument(
            "--name",
            type=str,
            default=None,
            required=True,
            help=f"Run named query. Available are: {','.join(QueryMaker.list_available())}",
        )
        parser.add_argument("--db-uri", type=str, default=None, help="Database uri")
        parser.add_argument(
            "--format", type=str, default=None, help="Output format of a query"
        )

    def execute(self, args):
        super().execute(args)

        app_settings = AppSettings.initialize()

        if args.db_uri is not None:
            app_settings.database.uri = args.db_uri

        database = SlurmMonitorDB(db_settings=app_settings.database)
        query_maker = QueryMaker(database)

        query = query_maker.create(args.name)
        result = df = query.execute()

        if not args.format:
            pass
        elif args.format.lower() == "json":
            result = df.to_json(orient="records")
        elif args.format.lower() == "csv":
            result = df.to_csv(header=True, sep=",", index=False)
        else:
            raise ValueError("The requested '{args.format}' is not supported")

        print(result)
