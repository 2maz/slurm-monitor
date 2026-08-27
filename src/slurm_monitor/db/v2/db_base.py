from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager, asynccontextmanager
from sqlalchemy.engine.url import URL, make_url
from sqlalchemy import (
        MetaData,
        create_engine,
        event,
        inspect,
        select,
        text,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import (
        AsyncSession,
        async_sessionmaker,
        create_async_engine,
)
import logging

from slurm_monitor.db.settings import DatabaseSettings
from slurm_monitor.db.v2.db_tables import TableBase
from slurm_monitor.db.v2.validation import Specification

logger = logging.getLogger(__name__)

# For performance reasons using half day as default history interval
INTERVAL_12H = 3600*12
INTERVAL_1DAY = 2*INTERVAL_12H
INTERVAL_1WEEK = 7*INTERVAL_1DAY
INTERVAL_2WEEKS = 14*INTERVAL_1DAY

DEFAULT_HISTORY_INTERVAL_IN_S = INTERVAL_12H

DB_POOL_SIZE = 25

# Above this row count, a plain (non-upsert) bulk insert switches to using COPY instead of
# a multi-row INSERT statement
# Particularly implemented to handle large batches of SampleProcess/SampleDisk rows
COPY_ROW_THRESHOLD = 2000

def create_url(url_str: str, username: str | None, password: str | None) -> URL:
    url = make_url(url_str)

    if url.get_dialect().name != "sqlite":
        assert url.username or username
        assert url.password or password

        url = url.set(
            username=url.username or username, password=url.password or password
        )
    return url


def _listify(obj_or_list):
    return obj_or_list if isinstance(obj_or_list, (tuple, list)) else [obj_or_list]

class Database:
    def __init__(self, db_settings: DatabaseSettings):
        # kept so callers can construct another, independent `Database` of
        # the same kind against the same target, e.g. a dedicated
        # connection per consumer thread
        self.db_settings = db_settings

        db_url = self.db_url = create_url(
            db_settings.uri, db_settings.user, db_settings.password
        )

        spec = Specification()
        spec.augment(TableBase.metadata.tables)

        engine_kwargs = {}
        self.engine = create_engine(db_url, **engine_kwargs)
        logger.info(
            f"Database with dialect: '{db_url.get_dialect().name}' detected - uri: {db_settings.uri}."
        )

        if db_url.get_dialect().name == "timescaledb":
            @event.listens_for(self.engine.pool, "connect")
            def _set_sqlite_params(dbapi_connection, *args):
                cursor = dbapi_connection.cursor()
                cursor.execute("CREATE EXTENSION IF NOT EXISTS hstore;")
                cursor.close()

        self.session_factory = sessionmaker(self.engine, expire_on_commit=False)


        self._metadata = MetaData()
        self._metadata.tables = {}
        self._metadata.bind = self.engine

        for attr in dir(type(self)):
            v = getattr(self, attr)
            if isinstance(v, type) and issubclass(v, TableBase):
                self._metadata.tables[v.__tablename__] = TableBase.metadata.tables[
                    v.__tablename__
                ]

        if db_settings.create_missing:
            self._metadata.create_all(self.engine)

        async_db_url = db_url
        if db_settings.uri.startswith("timescaledb://"):
            async_db_url = create_url(
                    db_settings.uri.replace("timescaledb:","timescaledb+asyncpg:"),
                    db_settings.user,
                    db_settings.password
            )

        self.async_engine = create_async_engine(
            async_db_url, pool_size=DB_POOL_SIZE, **engine_kwargs
        )
        self.async_session_factory = async_sessionmaker(
            self.async_engine, expire_on_commit=False
        )

        #from sqlalchemy_schemadisplay import create_schema_graph
        ## create the pydot graph object by autoloading all tables via a bound metadata object
        #graph = create_schema_graph(
        #   engine=self.engine,
        #   metadata=self._metadata,
        #   show_datatypes=True, # The image would get nasty big if we'd show the datatypes
        #   show_indexes=False, # ditto for indexes
        #   rankdir='LR', # From left to right (instead of top to bottom)
        #   concentrate=True # Don't try to join the relation lines together
        #)
        #graph.write_png('/tmp/dbschema.png') # write out the file

    def clone(self) -> "Database":
        """
        Construct a new, independent instance of this database - own engine
        and connection pools - against the same settings.

        Use this to give each of several concurrent consumers (e.g. one per
        thread) its own connection instead of sharing this instance's, and
        `dispose()` it once that consumer is done.

        Returns:
            A new instance of `type(self)` built from this instance's `db_settings`.
        """
        return type(self)(self.db_settings)

    async def dispose(self):
        """
        Dispose this database's connection pools (sync and async engines).

        Intended for a `Database` created for a single, short-lived purpose
        (e.g. one per consumer thread) that should release its connections
        once that purpose is done, rather than for the long-lived instance
        an application constructs once at startup.
        """
        self.engine.dispose()
        await self.async_engine.dispose()

    def get_column_description(self, table, column) -> str | None:
        """
        Get a table's column description aka comment

        :return Column description or None
        """
        statement = f"""
            SELECT description FROM pg_catalog.pg_description
                WHERE objsubid = (
                    SELECT ordinal_position FROM information_schema.columns
                        WHERE table_name='{table}' AND column_name='{column}'
                    )
                    and objoid = (
                        SELECT oid FROM pg_class WHERE relname='{table}' and relnamespace =
                            (
                                SELECT oid FROM pg_catalog.pg_namespace
                                    WHERE nspname = 'public'
                            )
                    );
        """
        with self.make_session() as session:
            result = session.execute(text(statement))
            description = result.fetchall()
            if description:
                return description[0][0]
            return None


    def insert(self, db_obj):
        with self.make_writeable_session() as session:
            session.add_all(_listify(db_obj))

    def insert_or_update(self, db_obj):
        with self.make_writeable_session() as session:
            for obj in _listify(db_obj):
                session.merge(obj)

    async def insert_async(self, db_obj, ignore_integrity_errors: bool = False):
        """
        Insert one or more rows in a single transaction - one commit (round
        trip + fsync) for the whole batch instead of one per row.

        Rows are grouped by table and written with one bulk INSERT statement
        per table (one more round trip, not one per row). If a table's bulk
        statement fails (e.g. a constraint conflict when not ignoring those,
        or a data error such as a malformed field), that table's rows are
        retried one at a time - each behind its own SAVEPOINT - so a single
        bad row is skipped on its own without discarding the rest of an
        otherwise-valid batch.

        Example:

        ```python
        await db.insert_async([row_a, row_b, row_c])
        ```

        Args:
            db_obj: A single row, or a list of rows, to insert.
            ignore_integrity_errors: When True, a row rejected for
                conflicting with existing data (e.g. re-ingesting historic
                data) is skipped silently. When False (default), it is
                skipped too, but logged as a warning.
        """
        async with self.make_writeable_async_session() as session:
            for table_cls, rows in self._group_by_table(db_obj).items():
                if await self._try_bulk_write(session, table_cls, rows, add=True, ignore_integrity_errors=ignore_integrity_errors):
                    continue

                for obj in rows:
                    await self._insert_row(session, obj, add=True, ignore_integrity_errors=ignore_integrity_errors)

    async def insert_or_update_async(self, db_obj, ignore_integrity_errors: bool = False):
        """
        Insert-or-update (merge) one or more rows in a single transaction -
        one commit (round trip + fsync) for the whole batch instead of one
        per row.

        Rows are grouped by table and written with one bulk upsert statement
        (`INSERT ... ON CONFLICT DO UPDATE`) per table (one more round trip,
        not one per row). If a table's bulk statement fails (e.g. a data
        error such as a malformed field), that table's rows are retried one
        at a time - each behind its own SAVEPOINT - so a single bad row is
        skipped on its own without discarding the rest of an otherwise-valid
        batch.

        Example:

        ```python
        await db.insert_or_update_async([row_a, row_b, row_c])
        ```

        Args:
            db_obj: A single row, or a list of rows, to merge.
            ignore_integrity_errors: When True, a row rejected for
                conflicting with existing data (e.g. re-ingesting historic
                data) is skipped silently. When False (default), it is
                skipped too, but logged as a warning.
        """
        async with self.make_writeable_async_session() as session:
            for table_cls, rows in self._group_by_table(db_obj).items():
                if await self._try_bulk_write(session, table_cls, rows, add=False, ignore_integrity_errors=ignore_integrity_errors):
                    continue

                # Use by-row commit as fallback, when bulk write fails
                logger.warning("Falling back to inserting by row")
                for obj in rows:
                    await self._insert_row(session, obj, add=False, ignore_integrity_errors=ignore_integrity_errors)

    @staticmethod
    def _group_by_table(db_obj) -> dict[type, list]:
        """
        Group rows by their concrete table class - one bulk statement targets
        one table, so rows destined for different tables can't share a
        statement.
        """
        groups: dict[type, list] = {}
        for obj in _listify(db_obj):
            groups.setdefault(type(obj), []).append(obj)
        return groups

    @staticmethod
    def _bulk_values(table_cls: type, rows: list, resolve_defaults: bool) -> list[dict]:
        """
        Convert `rows` into value dicts for a bulk INSERT/COPY.

        Args:
            resolve_defaults: True for a plain insert (a brand-new row) -
                fills any column left unset with its configured client-side
                default (`db_tables.Column()` gives nearly every column
                `nullable=False` plus a default, e.g. `default=0`), matching
                what `session.add(obj)` does automatically at flush time.
                A raw attribute read instead sees an unset attribute as
                `None`, which sends an explicit NULL and fails that same
                NOT NULL constraint a normal insert would never hit - and
                postgres has no *server-side* default to fall back on here
                (`column.default` is Python-side only), so omitting the
                column doesn't help either.

                False for an upsert - a stub row (e.g. `ensure_node()`'s
                placeholder `Node`) must not clobber real data already
                persisted in columns it never touched. `session.merge()`
                (used by the per-row fallback) only updates attributes
                actually set on the source object, leaving the rest of the
                existing row alone; resolving defaults here and pushing them
                through `ON CONFLICT DO UPDATE SET col = excluded.col` would
                instead overwrite already-good columns with blank defaults.
                Unset columns are left out (None) instead, same as before -
                a genuinely new row with an unset non-nullable column simply
                falls back to the per-row path, same as it always has.
        """
        column_defaults = {}
        if resolve_defaults:
            for name, column in table_cls.__table__.columns.items():
                default = column.default
                if default is not None and (default.is_scalar or default.is_callable):
                    column_defaults[name] = default

        all_columns = table_cls.__table__.columns.keys()
        values = []

        for row in rows:
            # known_columns() filters to actual columns of the table, rather
            # than relying on a "doesn't start with _sa_" guess to strip out
            # SQLAlchemy's own instance-state bookkeeping.
            set_attrs = table_cls.known_columns(**inspect(row).dict)
            values.append({
                name: set_attrs.get(name, Database._resolve_default(column_defaults.get(name)))
                for name in all_columns
            })

        return values

    @staticmethod
    def _resolve_default(default) -> any:
        """
        Resolve a `sqlalchemy.Column.default` to its actual value - `None` if
        there is none, the scalar itself for a scalar default, or the result
        of calling it for a callable default (e.g. `default=dt.datetime.now`
        - evaluated per row, so each row gets its own "now" rather than one
        timestamp shared across the whole batch).
        """
        if default is None:
            return None
        return default.arg({}) if default.is_callable else default.arg

    @staticmethod
    async def _try_bulk_write(session: AsyncSession, table_cls: type, rows: list, add: bool, ignore_integrity_errors: bool) -> bool:
        """
        Attempt one bulk INSERT (add=True) or upsert (add=False, `INSERT ...
        ON CONFLICT DO UPDATE`) statement covering all of `rows`.
        The attempt runs behind its own SAVEPOINT so that on failure it rolls
        back to a clean point (rather than leaving the session unable to
        proceed) and the caller can fall back to inserting rows one at a time.

        Returns:
            bool: True if the bulk statement succeeded, False if it failed
                and the caller should fall back to per-row writes for `rows`.
        """
        values = Database._bulk_values(table_cls, rows, resolve_defaults=add)

        # Upserts still need the regular ON CONFLICT DO UPDATE statement
        # below - COPY has no equivalent. A large plain insert uses COPY
        # either way: straight into the table when conflicts aren't
        # expected/tolerated, or via a staging table first when they are
        # (ignore_integrity_errors) - COPY itself has no conflict handling,
        # so loading a large "some of this may already exist" batch straight
        # into the table would abort the whole COPY on the first conflict,
        # discarding every row in the batch - including the genuinely new
        # ones - not just the conflicting ones.
        if add and len(values) > COPY_ROW_THRESHOLD:
            if ignore_integrity_errors:
                return await Database._copy_ignore_conflicts(session, table_cls, values)
            return await Database._copy(session, table_cls, values)

        def build_stmt(chunk: list[dict]):
            stmt = pg_insert(table_cls).values(chunk)
            if add:
                if ignore_integrity_errors:
                    return stmt.on_conflict_do_nothing(index_elements=table_cls.primary_key_columns())
                # else: plain insert - let a conflict fail the statement so
                # the per-row fallback can isolate and report the offending
                # row(s).
                return stmt

            update_cols = table_cls.non_primary_key_columns()
            return stmt.on_conflict_do_update(
                index_elements=table_cls.primary_key_columns(),
                set_={c: stmt.excluded[c] for c in update_cols},
            )

        try:
            async with session.begin_nested():
                # postgres/asyncpg hard-caps a single statement at 32767
                # bound parameters, and a multi-row VALUES (...), (...)
                # binds one per (row, column) pair - a wide table (e.g.
                # SampleSlurmJob's ~30 columns) can hit that at well under
                # COPY_ROW_THRESHOLD rows, and upserts never use COPY at all
                # (ON CONFLICT DO UPDATE has no COPY equivalent), so this
                # applies regardless of row count or add/upsert.
                for chunk in Database._chunk_values(values, table_cls):
                    await session.execute(build_stmt(chunk))
            return True
        except Exception as e:
            logger.warning(f"Bulk write of {len(rows)} {table_cls.__tablename__} row(s) failed -- {e}")
            return False

    # postgres/asyncpg's hard limit on bound parameters in a single statement
    POSTGRES_MAX_QUERY_PARAMS = 32767

    @staticmethod
    def _chunk_values(values: list[dict], table_cls: type) -> list[list[dict]]:
        """
        Split `values` into chunks that stay under
        `POSTGRES_MAX_QUERY_PARAMS` bound parameters per statement, based on
        `table_cls`'s column count.
        """
        if not values:
            return [values]

        num_columns = len(table_cls.__table__.columns)
        max_rows = max(1, Database.POSTGRES_MAX_QUERY_PARAMS // num_columns)
        return [values[i:i + max_rows] for i in range(0, len(values), max_rows)]

    @staticmethod
    async def _asyncpg_connection(session: AsyncSession):
        """
        The raw asyncpg connection currently bound to `session` - needed to
        reach asyncpg-native features (COPY) that have no SQLAlchemy Core
        equivalent. It's the same physical connection/transaction `session`
        is using, so anything run on it still participates in whatever
        SAVEPOINT is currently open on `session`.
        """
        connection = await session.connection()
        raw_connection = await connection.get_raw_connection()
        return raw_connection.driver_connection

    @staticmethod
    async def _copy(session: AsyncSession, table_cls: type, values: list[dict]) -> bool:
        """
        Bulk-load `values` into `table_cls`'s table via postgres COPY, using
        asyncpg's native (typed, non-CSV) copy support directly on the
        connection already bound to `session` - so it still runs inside
        `session`'s current transaction, behind its own SAVEPOINT, and rolls
        back cleanly on failure just like the regular bulk-statement path.

        Returns:
            bool: True if COPY succeeded, False if it failed and the caller
                should fall back to per-row writes for `values`.
        """
        columns = list(values[0].keys())
        records = [tuple(row[c] for c in columns) for row in values]

        try:
            async with session.begin_nested():
                asyncpg_connection = await Database._asyncpg_connection(session)
                await asyncpg_connection.copy_records_to_table(
                    table_cls.__tablename__, records=records, columns=columns
                )
            return True
        except Exception as e:
            logger.warning(f"COPY of {len(records)} {table_cls.__tablename__} row(s) failed -- {e}")
            return False

    @staticmethod
    async def _copy_ignore_conflicts(session: AsyncSession, table_cls: type, values: list[dict]) -> bool:
        """
        Bulk-load `values` into `table_cls`'s table via postgres COPY while
        still tolerating rows that conflict with existing data (same intent
        as `ON CONFLICT DO NOTHING`).

        COPY itself has no conflict-handling clause, so this loads into a
        per-connection temp staging table first (no constraints there, so it
        never itself fails on a conflict), then folds it into the real table
        with one server-side `INSERT ... SELECT ... ON CONFLICT DO NOTHING` -
        two round trips total, with conflict resolution happening inside
        postgres over the whole batch at once. The alternative - COPYing
        straight into the table - would abort the entire COPY on the first
        conflicting row, discarding every row in the batch, including the
        genuinely new ones, for what should be the routine case (re-ingesting
        historic data that partially overlaps what's already stored).

        Returns:
            bool: True if this succeeded, False if it failed and the caller
                should fall back to per-row writes for `values`.
        """
        columns = list(values[0].keys())
        records = [tuple(row[c] for c in columns) for row in values]
        column_list = ", ".join(f'"{c}"' for c in columns)
        conflict_target = ", ".join(f'"{c}"' for c in table_cls.primary_key_columns())
        staging_table = f"_copy_staging_{table_cls.__tablename__}"

        try:
            async with session.begin_nested():
                asyncpg_connection = await Database._asyncpg_connection(session)
                # per-connection (temp tables aren't visible across
                # connections), so reused as-is if this table is COPY'd
                # again later on the same connection
                await asyncpg_connection.execute(
                    f'CREATE TEMP TABLE IF NOT EXISTS "{staging_table}" '
                    f"(LIKE {table_cls.__tablename__} INCLUDING DEFAULTS)"
                )
                await asyncpg_connection.execute(f'TRUNCATE "{staging_table}"')
                await asyncpg_connection.copy_records_to_table(
                    staging_table, records=records, columns=columns
                )
                await asyncpg_connection.execute(
                    f"INSERT INTO {table_cls.__tablename__} ({column_list}) "
                    f'SELECT {column_list} FROM "{staging_table}" '
                    f"ON CONFLICT ({conflict_target}) DO NOTHING"
                )
            return True
        except Exception as e:
            logger.warning(f"COPY (ignore-conflicts) of {len(records)} {table_cls.__tablename__} row(s) failed -- {e}")
            return False

    @staticmethod
    async def _insert_row(session: AsyncSession, obj, add: bool, ignore_integrity_errors: bool):
        """
        Add (add=True) or merge (add=False) a single row behind its own
        SAVEPOINT within `session`'s already-open transaction, so a failure
        on this row only rolls back this row, not the whole batch.
        """
        try:
            async with session.begin_nested():
                if add:
                    session.add(obj)
                else:
                    await session.merge(obj)
                await session.flush()
        except IntegrityError as e:
            if not ignore_integrity_errors:
                logger.warning(f"Row rejected due to integrity error, skipping: {obj=} -- {e}")
        except Exception as e:
            logger.warning(f"Row failed, skipping: {obj=} -- {e}")

    @contextmanager
    def make_session(self):
        session = self.session_factory()
        try:
            yield session
            if session.deleted or session.dirty or session.new:
                raise RuntimeError(
                    "Found potentially modified state in a non-writable session"
                )
        except:
            session.rollback()
            raise
        finally:
            session.close()

    @contextmanager
    def make_writeable_session(self):
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    @asynccontextmanager
    async def make_async_session(self) -> AsyncSession:
        session = self.async_session_factory()
        try:
            yield session
            if session.deleted or session.dirty or session.new:
                raise Exception(
                    "Found potentially modified state in a non-writable session"
                )
        except:
            await session.rollback()
            raise
        finally:
            await session.close()

    @asynccontextmanager
    async def make_writeable_async_session(self) -> AsyncSession:
        session = self.async_session_factory()
        try:
            yield session
            await session.commit()
        except:
            await session.rollback()
            raise
        finally:
            await session.close()

    async def _fetch_async(self, db_cls,
            where=None,
            limit: int | None = None,
            order_by=None,
            _reduce=None, _unpack=True):
        query = select(*_listify(db_cls))
        if where is not None:
            query = query.where(where)
        if limit is not None:
            query = query.limit(limit)
        if order_by is not None:
            query = query.order_by(order_by)

        async with self.make_async_session() as session:
            query_results = await session.execute(query)

            result = [x for x in query_results.all()] if _reduce is None else _reduce(query_results)
            if _unpack and not isinstance(db_cls, (tuple, list)):
                result = [r[0] for r in result]

            return result

    async def fetch_all_async(self, db_cls, where=None, **kwargs):
        return await self._fetch_async(db_cls, where=where, **kwargs)

    async def fetch_first_async(self, db_cls, where=None, order_by=None):
        query = select(*_listify(db_cls))
        if where is not None:
            query = query.where(where)
        if order_by is not None:
            query = query.order_by(order_by)

        query = query.limit(1)

        async with self.make_async_session() as session:
            results = [x[0] for x in await session.execute(query)]
            if results:
                return results[0]
            else:
                raise RuntimeError("No entries. Could not pick first")

    async def fetch_latest_async(self, db_cls, where=None):
       return await self.fetch_first_async(db_cls=db_cls, where=where, order_by=db_cls.time.desc())
