from argparse import ArgumentParser
import logging

from slurm_monitor.db_operations import DBManager
from slurm_monitor.cli.base import BaseParser
from slurm_monitor.app_settings import AppSettings

logger = logging.getLogger(__name__)


class DBParser(BaseParser):
    def __init__(self, parser: ArgumentParser):
        super().__init__(parser=parser)

        parser.add_argument("--db-schema-version",
                            choices=["v1", "v2"],
                            default="v2",
                            help="Database schema version to use (default is 'v2')"
                            )

        parser.add_argument("--db-uri",
                            type=str,
                            help="Database uri"
                            )

        parser.add_argument("--init",
                            action="store_true",
                            help="Initialize the database, i.e., create schemas etc"
                            )

        parser.add_argument("--apply-changes",
                            action="store_true",
                            help="Apply changes to the table of the current database"
                            )

        parser.add_argument("--diff",
                            action="store_true",
                            help="Show only diff lines"
                            )

        parser.add_argument("--insert-test-samples",
                            metavar="CLUSTER",
                            nargs="+",
                            type=str,
                            required=False,
                            default=None,
                            help="Insert test data for a given cluster"
        )


    def execute(self, args):
        super().execute(args)

        app_settings = AppSettings.initialize()
        app_settings.db_schema_version = args.db_schema_version

        if args.db_uri:
            app_settings.database.uri = args.db_uri

        if args.insert_test_samples:
            from slurm_monitor.db.v2.db_testing import TestDBConfig, create_test_db
            test_db_config = TestDBConfig(cluster_names=args.insert_test_samples)
            create_test_db(uri=app_settings.database.uri, config=test_db_config)

        initial_status = DBManager.get_status(app_settings.database.uri)

        app_settings.database.create_missing = args.apply_changes or args.init
        db = DBManager.get_database(app_settings)

        schema = DBManager.get_schema(db)
        tables_in_schema = schema.keys()

        deprecated_tables = set(initial_status.keys()) - tables_in_schema
        print("Summary: ")
        if args.apply_changes or args.init:
            new_status = DBManager.get_status(app_settings.database.uri)
            added_tables = set(new_status.keys()) - set(initial_status.keys())

            DBManager.print_status(schema, new_status, diff=args.diff)
            changes = DBManager.apply_changes(db, schema, new_status)

            print(f"    added tables: {[x for x in added_tables]}")
            for category, details in changes.items():
                print(f"        {category} changes:")
                for x in sorted(details):
                    print(f"           - {x}")
            print(f"    deprecated (not in schema): {[x for x in deprecated_tables]}")

        else:
            # what tables have not been defined in the schema
            to_be_added_tables = tables_in_schema - set(initial_status.keys())
            DBManager.print_status(schema, initial_status, diff=args.diff)

            print()
            print(f"    to be added: {[x for x in to_be_added_tables]}")
            print(f"    deprecated (not in schema): {[x for x in deprecated_tables]}")
