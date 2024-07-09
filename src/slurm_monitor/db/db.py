import logging
import sqlalchemy
from contextlib import contextmanager
from typing import ClassVar, Any
import datetime as dt

from pydantic import ConfigDict, BaseModel
from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Text,
    event,
    create_engine,
    insert,
    inspect,
    types
)
from sqlalchemy.orm import DeclarativeMeta, as_declarative, sessionmaker
from sqlalchemy.engine.url import URL, make_url

from slurm_monitor.db.db_tables import (
       TableBase,
       Nodes,
       Jobs,
       GPUStatus
)

logger = logging.getLogger(__name__)


class DatabaseSettings(BaseModel):
    user: str | None = None
    password: str | None = None
    uri: str = "sqlite://"

    create_missing: bool = True


def _listify(obj_or_list):
    return obj_or_list if isinstance(obj_or_list, (tuple, list)) else [obj_or_list]

def create_url(url_str: str, username: str | None , password: str | None) -> URL:
    url = make_url(url_str)

    if url.get_dialect().name != "sqlite":
        assert url.username or username
        assert url.password or password

        url = url.set(
                username=url.username or username,
                password=url.password or password
        )
    return url

class Database:
    def __init__(self, db_settings: DatabaseSettings):
        db_url = self.db_url = create_url(db_settings.uri, db_settings.user, db_settings.password)

        engine_kwargs = {}
        if db_settings.uri == "sqlite://":
            from sqlalchemy.pool import StaticPool

            engine_kwargs["connect_args"] = dict(check_same_thread=False)
            engine_kwargs["poolclass"] = StaticPool

        self.engine = create_engine(db_url, **engine_kwargs)
        logger.info(f"Database with dialect: '{db_url.get_dialect().name}' detectred - uri: {db_settings.uri}.")

        if db_url.get_dialect().name == "sqlite":
            @event.listens_for(self.engine.pool, "connect")
            def _set_sqlite_params(dbapi_connection, *args):
                cursor = dbapi_connection.cursor()
                cursor.execute("PRAGMA foreign_keys=ON;")
                cursor.close()

        self.session_factory = sessionmaker(self.engine, expire_on_commit=False)

        self._metadata = MetaData()
        self._metadata.tables = {}

        for attr in dir(type(self)):
            v = getattr(self, attr)
            if isinstance(v, type) and issubclass(v, TableBase):
                self._metadata.tables[v.__tablename__] = TableBase.metadata.tables[v.__tablename__]

        if db_settings.create_missing:
            self._metadata.create_all(self.engine)


    @contextmanager
    def make_session(self):
        session = self.session_factory()
        try:
            yield session
            if session.deleted or session.dirty or session.new:
                raise Exception("Found potentially modified state in a non-writable session")
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

    def _fetch(self, db_cls, where=None, _reduce=None, _unpack=True):
        with self.make_session() as session:
            query = session.query(*_listify(db_cls))
            if where is not None:
                query = query.filter(where)

            result = list(query.all()) if _reduce is None else _reduce(query)
        if _unpack and not isinstance(db_cls, (tuple, list, DeclarativeMeta)):
            result = [r[0] for r in result]
        return result

    def fetch_all(self, db_cls, where=None):
        return self._fetch(db_cls, where=where, _reduce=lambda q: list(q.all()))

    def insert(self, db_obj):
        with self.make_writeable_session() as session:
            session.add_all(_listify(db_obj))

    def insert_or_update(self, db_obj):
        with self.make_writeable_session() as session:
            for obj in _listify(db_obj):
                session.merge(obj)

class SlurmMonitorDB(Database):
    Nodes = Nodes
    GPUStatus = GPUStatus




if __name__ == "__main__":
    import pdb

    db_settings = DatabaseSettings(
            user="",
            password="",
            uri="sqlite:////tmp/test.sqlite"
    )
    db = SlurmMonitorDB(db_settings=db_settings)

    value = {'name': 'Tesla V100-SXM3-32GB', 'uuid': 'GPU-ad466f2f-575d-d949-35e0-9a7d912d974e', 'power.draw': '313.07', 'temperature.gpu': '52', 'utilization.gpu': '82', 'utilization.memory': '28', 'memory.used': '4205', 'memory.free': '28295'}

    db.insert_or_update(Nodes(name="g001"))

    db.insert(GPUStatus(
        name=value["name"],
        uuid=value["uuid"],
        node="g001",
        power_draw=value["power.draw"],
        temperature_gpu=value["temperature.gpu"],
        utilization_memory=value["utilization.memory"],
        utilization_gpu=value["utilization.gpu"],
        memory_used=value["memory.used"],
        memory_free=value["memory.free"],
        timestamp=dt.datetime.now()))

    print(db.fetch_all(GPUStatus))






