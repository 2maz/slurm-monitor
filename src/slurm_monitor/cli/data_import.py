from argparse import ArgumentParser
import json
from pathlib import Path

from slurm_monitor.cli.base import BaseParser
from slurm_monitor.db.v2.data_subscriber import DBJsonImporter

from slurm_monitor.db.v2.db import ClusterDB
from slurm_monitor.app_settings import AppSettings


class ImportParser(BaseParser):
    def __init__(self, parser: ArgumentParser):
        super().__init__(parser=parser)

        parser.add_argument("--db-uri",
            type=str,
            default=None,
            help="Database uri"
        )
        parser.add_argument("--fake-timeseries",
            default=False,
            action="store_true",
            help="Fake timeseries data - to have recent samples"
        )

        parser.add_argument("-f", "--file",
            type=str,
            default=None,
            help="Sample data (in json format) to import"
        )

    def execute(self, args):
        super().execute(args)

        app_settings = AppSettings.initialize()

        if args.db_uri is not None:
            app_settings.database.uri = args.db_uri

        db = ClusterDB(db_settings=app_settings.database)
        if args.file is not None:
            if not Path(args.file).exists():
                raise FileNotFoundError(f"Could not find file: {args.file}")

            with open(args.file, 'r') as f:
                data = json.load(f)

            importer = DBJsonImporter(db)
            print(f"Trying to import data from: {args.file}")
            importer.insert(data)

        if args.fake_timeseries:
            db.fake_timeseries()
