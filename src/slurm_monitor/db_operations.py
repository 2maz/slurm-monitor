from logging import getLogger, Logger
import re
import sqlalchemy

from fastapi import HTTPException
from typing import ClassVar, Iterable
from slurm_monitor.app_settings import AppSettings
from slurm_monitor.db.v2.db_tables import ExtraIndexPrefix

logger: Logger = getLogger(__name__)


def interactive_yes_no(prompt: str) -> bool:
    while True:
        answer = input(f"{prompt} [y/N]")
        if answer.lower() in ["y", "yes"]:
            return True
        if answer.lower() in ["n", "no"]:
            return False

        if answer == "":
            return False


class DBManager:
    COLUMNS: str = "columns"
    INDEXES: str = "indexes"

    _databases: ClassVar[dict[str, any]] = {}

    @classmethod
    def clear_cache(cls):
        cls._databases.clear()

    @classmethod
    def get_database(cls, app_settings: AppSettings | None = None):
        if app_settings is None:
            app_settings = AppSettings.get_instance()

        if app_settings.db_schema_version in cls._databases:
            return cls._databases[app_settings.db_schema_version]

        logger.info(f"Loading database with: {app_settings.database} -- existing {cls._databases}")
        try:
            if app_settings.db_schema_version == "v1":
                from slurm_monitor.db.v1.db import SlurmMonitorDB

                db = SlurmMonitorDB(app_settings.database)
            elif app_settings.db_schema_version == "v2":
                from slurm_monitor.db.v2.db import ClusterDB

                db = ClusterDB(app_settings.database)
            else:
                raise RuntimeError("AppSettings.db_schema_version is not set")

            cls._databases[app_settings.db_schema_version] = db
            return db
        except sqlalchemy.exc.OperationalError:
            raise HTTPException(
                status_code=500,
                detail=f"Cannot access monitor database - {app_settings.database.uri}",
            )

    @classmethod
    def get_status(cls, db_uri):
        from sqlalchemy import inspect, create_engine

        engine = create_engine(db_uri)
        inspector = inspect(engine)

        status = {}
        for table in inspector.get_table_names():
            columns = inspector.get_columns(table)
            indexes = inspector.get_indexes(table)
            status[table] = {
                cls.COLUMNS: {x["name"]: x for x in columns},
                cls.INDEXES: {x["name"]: x for x in indexes},
            }
        return status

    @classmethod
    def get_schema(cls, db):
        schema = {}
        for table_name, table in db._metadata.tables.items():
            schema[table_name] = {
                cls.COLUMNS: {x.name: x for x in table.columns},
                cls.INDEXES: {x.name: x for x in table.indexes},
            }
        return schema

    @classmethod
    def get_custom_indexes(cls, indexes: Iterable[str]) -> set[str]:
        return set([x for x in indexes if x.startswith(ExtraIndexPrefix)])

    @classmethod
    def print_status(cls, schema, status, diff: bool = False):
        if diff:
            print("Tables (Diff)")
        else:
            print("Tables")

        for table, table_metadata in status.items():
            columns = table_metadata[cls.COLUMNS]
            indexes = table_metadata[cls.INDEXES]

            prefix = "  "
            add = {}
            remove = {}
            modify = {}

            for i in [cls.COLUMNS, cls.INDEXES]:
                add[i] = {}
                remove[i] = {}
                modify[i] = {}

            if table not in schema:
                prefix = "!-"
            else:
                columns_in_schema = set(schema[table][cls.COLUMNS].keys())
                add[cls.COLUMNS] = columns_in_schema - set(columns.keys())
                remove[cls.COLUMNS] = set(columns.keys()) - columns_in_schema

                index_in_schema = set(schema[table][cls.INDEXES].keys())
                add[cls.INDEXES] = cls.get_custom_indexes(
                    index_in_schema - set(indexes.keys())
                )
                remove[cls.INDEXES] = cls.get_custom_indexes(
                    set(indexes.keys()) - index_in_schema
                )

                for column in columns_in_schema:
                    if column in add[cls.COLUMNS] or column in remove[cls.COLUMNS]:
                        continue

                    for attribute in ["comment"]:
                        attribute_in_db = status[table][cls.COLUMNS][column][attribute]
                        attribute_in_schema = getattr(
                            schema[table][cls.COLUMNS][column], attribute
                        )

                        if attribute_in_db != attribute_in_schema:
                            if column not in modify[cls.COLUMNS]:
                                modify[cls.COLUMNS][column] = [attribute]
                            else:
                                modify[cls.COLUMNS][column].append(attribute)

                            prefix = "!~"
            if diff:
                with_changes = False
                for i in [cls.COLUMNS, cls.INDEXES]:
                    if add[i] or remove[i] or modify[i]:  # or table in schema:
                        with_changes = True

                if not with_changes:
                    continue

            print(f"{prefix}  {table}")
            for column_name in sorted(list(columns.keys())):
                c_prefix = prefix
                if c_prefix.strip() == "":
                    if column_name in remove[cls.COLUMNS]:
                        c_prefix = "!-"
                    elif diff:
                        # create a compact view in 'diff' mode
                        continue

                column = columns[column_name]

                if table in schema:
                    comment = getattr(
                        schema[table][cls.COLUMNS][column_name], "comment"
                    )
                else:
                    comment = status[table][cls.COLUMNS][column_name]["comment"]

                if comment is not None:
                    comment = comment.strip()
                print(
                    f"{c_prefix}      {column['name'].ljust(20)} {str(column['type']).ljust(20)} {comment}"
                )

            for column_name in add[cls.COLUMNS]:
                schema_column = schema[table][cls.COLUMNS][column_name]
                print(
                    f"!+      {column_name.ljust(20)} {str(schema_column.type).ljust(20)} {schema_column.comment}"
                )

            print("        > indexes:")
            for index_name in sorted(list(indexes.keys())):
                c_prefix = prefix
                if c_prefix.strip() == "":
                    if index_name in remove[cls.INDEXES]:
                        c_prefix = "!-"
                    elif diff:
                        # create a compact view in 'diff' mode
                        continue

                index = indexes[index_name]
                print(
                    f"{c_prefix}              {index['name'].ljust(20)} columns: {','.join(index['column_names']).ljust(20)}"
                )

            for index_name in add[cls.INDEXES]:
                schema_index = schema[table][cls.INDEXES][index_name]
                columns = [x.name for x in schema_index.columns]
                print(
                    f"!+              {index_name.ljust(20)} columns: {','.join(columns).ljust(20)}"
                )

    @classmethod
    def apply_changes(cls, db, schema, status):
        return {
            cls.COLUMNS: cls.apply_column_changes(db, schema, status),
            cls.INDEXES: cls.apply_index_changes(db, schema, status),
        }

    @classmethod
    def apply_column_changes(cls, db, schema, status):
        added_columns = []
        for table, table_metadata in status.items():
            # check columns
            columns = table_metadata[cls.COLUMNS]

            add_columns = []
            if table in schema:
                columns_in_schema = set(schema[table][cls.COLUMNS].keys())
                add_columns = columns_in_schema - set(columns.keys())
            else:
                continue

            for column_name in add_columns:
                column = schema[table][cls.COLUMNS][column_name]

                dialect_type = column.type.dialect_impl(
                    db.engine.dialect
                ).__visit_name__
                if dialect_type == "ARRAY":
                    item_type = column.type.item_type.dialect_impl(
                        db.engine.dialect
                    ).__visit_name__
                    typename = f"{item_type}[]"
                elif dialect_type == "string":
                    typename = str(column.type)
                elif dialect_type == "datetime":
                    typename = db.engine.dialect.type_compiler.process(column.type)
                else:
                    typename = dialect_type

                alter_stmt = f"""
                        ALTER TABLE {table} ADD COLUMN {column_name}
                        {typename} NULL DEFAULT NULL
                """
                with db.make_writeable_session() as session:
                    session.execute(sqlalchemy.text(alter_stmt))

                # escape quotes (ensure only single quote is used)
                comment = ""
                if column.comment:
                    comment = column.comment.strip()
                    comment = re.sub("'{2,}", "'", comment)
                    comment = re.sub("'", "''", comment)

                    alter_comment_stmt = f"""
                            COMMENT ON COLUMN {table}."{column_name}" IS '{comment}'
                    """
                    with db.make_writeable_session() as session:
                        session.execute(sqlalchemy.text(alter_comment_stmt))

                print(
                    f"Adding column: {column_name.ljust(20)} {schema[table][cls.COLUMNS][column_name].type} with comment '{comment}'"
                )

                added_columns.append(f"{table}.{column_name}")

            for column in columns_in_schema:
                if column in add_columns:
                    continue

                # Ensure comments are update to date
                for attribute in ["comment"]:
                    attribute_in_db = status[table][cls.COLUMNS][column][attribute]
                    attribute_in_schema = getattr(
                        schema[table][cls.COLUMNS][column], attribute
                    )

                    if attribute_in_schema and attribute_in_db != attribute_in_schema:
                        attribute_in_schema = re.sub("'{2,}", "'", attribute_in_schema)
                        attribute_in_schema = re.sub("'", "''", attribute_in_schema)

                        alter_attribute_stmt = f"""
                                COMMENT ON COLUMN {table}."{column}" IS '{attribute_in_schema}'
                        """
                        logger.debug(alter_attribute_stmt)
                        with db.make_writeable_session() as session:
                            session.execute(sqlalchemy.text(alter_attribute_stmt))
        return added_columns

    @classmethod
    def apply_index_changes(cls, db, schema, status):
        added_indexes = []
        for table, table_metadata in status.items():
            indexes = table_metadata[cls.INDEXES]

            add_indexes = []
            if table in schema:
                indexes_in_schema = set(schema[table][cls.INDEXES].keys())
                add_indexes = indexes_in_schema - set(indexes.keys())
                remove_indexes = cls.get_custom_indexes(
                    set(indexes.keys()) - indexes_in_schema
                )
            else:
                continue

            for index_name in add_indexes:
                index_obj = [
                    x
                    for x in db._metadata.tables[table].indexes
                    if x.name == index_name
                ][0]
                index_obj.create(bind=db.engine)

                columns = [x.name for x in index_obj.columns]
                print(
                    f"Adding index: {index_name.ljust(20)} columns: {','.join(columns)}"
                )
                added_indexes.append(f"{table}.{index_name}")

            for index_name in remove_indexes:
                remove_index_statement = f"""
                    DROP INDEX {index_name}
                """
                logger.debug(remove_index_statement)
                interactive_yes_no(f"Remove index: {index_name}?")
                with db.make_writeable_session() as session:
                    session.execute(sqlalchemy.text(remove_index_statement))
        return added_indexes
