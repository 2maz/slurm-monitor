import os
from pydantic import BaseModel
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager, asynccontextmanager
from sqlalchemy.engine.url import URL, make_url
from sqlalchemy import (
    MetaData,
    create_engine,
    event,
    select,
    text,
)
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
import logging

from slurm_monitor.db.v2.db_tables import TableBase
from slurm_monitor.db.v2.validation import Specification

logger = logging.getLogger(__name__)

# For performance reasons using half day as default history interval
INTERVAL_12H = 3600 * 12
INTERVAL_1DAY = 2 * INTERVAL_12H
INTERVAL_1WEEK = 7 * INTERVAL_1DAY
INTERVAL_2WEEKS = 14 * INTERVAL_1DAY

DEFAULT_HISTORY_INTERVAL_IN_S = INTERVAL_12H

DB_POOL_SIZE = 25

def create_url(url_str: str, username: str | None, password: str | None) -> URL:
    url = make_url(url_str)

    if url.get_dialect().name != "sqlite":
        assert url.username or username
        assert url.password or password

        url = url.set(
            username=url.username or username, password=url.password or password
        )
    return url


class DatabaseSettings(BaseModel):
    user: str | None = None
    password: str | None = None
    uri: str = f"sqlite:///{os.environ['HOME']}/.slurm-monitor/slurm-monitor-db.sqlite"

    create_missing: bool = True


def _listify(obj_or_list):
    return obj_or_list if isinstance(obj_or_list, (tuple, list)) else [obj_or_list]


class Database:
    def __init__(self, db_settings: DatabaseSettings):
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
                db_settings.uri.replace("timescaledb:", "timescaledb+asyncpg:"),
                db_settings.user,
                db_settings.password,
            )

        self.async_engine = create_async_engine(
            async_db_url, pool_size=DB_POOL_SIZE, **engine_kwargs
        )
        self.async_session_factory = async_sessionmaker(
            self.async_engine, expire_on_commit=False
        )

        # from sqlalchemy_schemadisplay import create_schema_graph
        ## create the pydot graph object by autoloading all tables via a bound metadata object
        # graph = create_schema_graph(
        #   engine=self.engine,
        #   metadata=self._metadata,
        #   show_datatypes=True, # The image would get nasty big if we'd show the datatypes
        #   show_indexes=False, # ditto for indexes
        #   rankdir='LR', # From left to right (instead of top to bottom)
        #   concentrate=True # Don't try to join the relation lines together
        # )
        # graph.write_png('/tmp/dbschema.png') # write out the file

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

    async def _fetch_async(
        self,
        db_cls,
        where=None,
        limit: int | None = None,
        order_by=None,
        _reduce=None,
        _unpack=True,
    ):
        query = select(*_listify(db_cls))
        if where is not None:
            query = query.where(where)
        if limit is not None:
            query = query.limit(limit)
        if order_by is not None:
            query = query.order_by(order_by)

        async with self.make_async_session() as session:
            query_results = await session.execute(query)

            result = (
                [x for x in query_results.all()]
                if _reduce is None
                else _reduce(query_results)
            )
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
        return await self.fetch_first_async(
            db_cls=db_cls, where=where, order_by=db_cls.time.desc()
        )
