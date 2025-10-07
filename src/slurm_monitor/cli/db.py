from argparse import ArgumentParser

from slurm_monitor.cli.base import BaseParser
from slurm_monitor.app_settings import AppSettings
import slurm_monitor.db_operations as db_ops

def get_db_status(db_uri):
    from sqlalchemy import inspect, create_engine

    engine = create_engine(db_uri)
    inspector = inspect(engine)

    status = {}
    for table in inspector.get_table_names():
        status[table] = {x['name']: x for x in inspector.get_columns(table)}

    return status


def get_db_schema(db):
    schema = {}
    for table_name, table in db._metadata.tables.items():
        schema[table_name] = {x.name: x for x in table.columns}
    return schema

def print_status(schema, status, diff: bool = False):
        if diff:
            print("Tables (Diff)")
        else:
            print("Tables")

        for table, columns in status.items():
            prefix = "  "
            add_columns = []
            remove_columns = []

            if table not in schema:
                prefix = "!-"
            else:
                columns_in_schema = set(schema[table].keys())
                add_columns = columns_in_schema - set(columns.keys())
                remove_columns = set(columns.keys()) - columns_in_schema

            if diff and not (add_columns or remove_columns or table not in schema):
                continue

            print(f"{prefix}  {table}")
            for column_name in sorted(list(columns.keys())):
                c_prefix = prefix
                if c_prefix.strip() == "":
                    if column_name in remove_columns:
                        c_prefix = "!-"
                    elif diff:
                        # create a compact view in 'diff' mode
                        continue

                column = columns[column_name]
                print(f"{c_prefix}      {column['name'].ljust(20)} {column['type']}")

            for column_name in add_columns:
                print(f"!+      {column_name.ljust(20)} {schema[table][column_name].type}")

def apply_changes(db, schema, status):
    import sqlalchemy

    for table, columns in status.items():
        prefix = "  "
        add_columns = []

        if table in schema:
            columns_in_schema = set(schema[table].keys())
            add_columns = columns_in_schema - set(columns.keys())
        else:
            continue

        for column_name in add_columns:

            column = schema[table][column_name]
            dialect_type = column.type.dialect_impl(db.engine.dialect).__visit_name__
            if dialect_type == "ARRAY":
                item_type = column.type.item_type.dialect_impl(db.engine.dialect).__visit_name__
                typename = f"{item_type}[]"
            else:
                typename = dialect_type

            comment = column.comment.strip()
            alter_stmt = f"""
                    ALTER TABLE {table} ADD COLUMN {column_name}
                    {typename} NULL DEFAULT NULL
            """
            alter_comment_stmt = f"""
                    COMMENT ON COLUMN {table}.{column_name} IS '{comment}'
            """

            print(f"Adding column: {column_name.ljust(20)} {schema[table][column_name].type} with comment '{comment}'")

            with db.make_writeable_session() as session:
                session.execute(sqlalchemy.text(alter_stmt))
                session.execute(sqlalchemy.text(alter_comment_stmt))


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
            test_db_config  = TestDBConfig(cluster_names=args.insert_test_samples)
            create_test_db(uri=app_settings.database.uri, config=test_db_config)

        initial_status = get_db_status(app_settings.database.uri)

        app_settings.database.create_missing = args.apply_changes
        db = db_ops.get_database(app_settings)


        schema = get_db_schema(db)
        tables_in_schema = schema.keys()

        deprecated_tables = set(initial_status.keys()) - tables_in_schema
        if args.apply_changes:
            new_status = get_db_status(app_settings.database.uri)
            added_tables = set(new_status.keys()) - set(initial_status.keys())

            print_status(schema, new_status, diff=args.diff)
            apply_changes(db, schema, new_status)

            print()
            print(f"added: {[x for x in added_tables]}")
            print(f"deprecated (not in schema): {[x for x in deprecated_tables]}")
        else:
            # what tables have not been defined in the schema
            to_be_added_tables = tables_in_schema - set(initial_status.keys())
            print_status(schema, initial_status, diff=args.diff)

            print()
            print(f"to be added: {[x for x in to_be_added_tables]}")
            print(f"deprecated (not in schema): {[x for x in deprecated_tables]}")
