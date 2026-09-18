import csv
import io
import json
from argparse import ArgumentParser
from pprint import pprint

from slurm_monitor.app_settings import AppSettings
from slurm_monitor.cli.base import BaseParser
from slurm_monitor.db.v2.db import ClusterDB
from slurm_monitor.db.v2.query import QueryMaker


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
        parser.add_argument(
            "--db-uri",
            type=str,
            default=None,
            help="Database uri",
        )
        parser.add_argument(
            "--format",
            type=str,
            default=None,
            help="Output format of a query",
        )
        parser.add_argument(
            "--params",
            type=str,
            default=None,
            help="Query parameters",
        )

    def execute(self, args):
        super().execute(args)

        app_settings = AppSettings.initialize()

        if args.db_uri is not None:
            app_settings.database.uri = args.db_uri

        db = ClusterDB(db_settings=app_settings.database)
        query_maker = QueryMaker()

        query = query_maker.create(db=db, name=args.name)
        result: list[dict[str, object]] = query.execute(params=args.params)

        if not args.format:
            pprint(result)
        elif args.format.lower() == "json":
            out = json.dumps(result, indent=2)
            print(out)
        elif args.format.lower() == "csv":
            out = io.StringIO()
            writer = csv.writer(out)
            writer.writerows(result)
            out.seek(0)
            out = out.read()
            print(out)
        else:
            raise ValueError("The requested '{args.format}' is not supported")
