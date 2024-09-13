import logging
import os
import sqlalchemy
from contextlib import contextmanager
import datetime as dt

from pydantic import BaseModel
from sqlalchemy import MetaData, event, create_engine
from sqlalchemy.orm import DeclarativeMeta, sessionmaker
from sqlalchemy.engine.url import URL, make_url

from slurm_monitor.db.db_tables import GPUStatus, JobStatus, Nodes, TableBase
import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class DatabaseSettings(BaseModel):
    user: str | None = None
    password: str | None = None
    uri: str = f"sqlite:///{os.environ['HOME']}/.slurm-monitor/slurm-monitor-db.sqlite"

    create_missing: bool = True


def _listify(obj_or_list):
    return obj_or_list if isinstance(obj_or_list, (tuple, list)) else [obj_or_list]


def create_url(url_str: str, username: str | None, password: str | None) -> URL:
    url = make_url(url_str)

    if url.get_dialect().name != "sqlite":
        assert url.username or username
        assert url.password or password

        url = url.set(
            username=url.username or username, password=url.password or password
        )
    return url


class Database:
    def __init__(self, db_settings: DatabaseSettings):
        db_url = self.db_url = create_url(
            db_settings.uri, db_settings.user, db_settings.password
        )

        engine_kwargs = {}
        if db_settings.uri == "sqlite://":
            from sqlalchemy.pool import StaticPool

            engine_kwargs["connect_args"] = dict(check_same_thread=False, timeout=15)
            engine_kwargs["poolclass"] = StaticPool

        self.engine = create_engine(db_url, **engine_kwargs)
        logger.info(
            f"Database with dialect: '{db_url.get_dialect().name}' detected - uri: {db_settings.uri}."
        )

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
                self._metadata.tables[v.__tablename__] = TableBase.metadata.tables[
                    v.__tablename__
                ]

        if db_settings.create_missing:
            self._metadata.create_all(self.engine)

    @contextmanager
    def make_session(self):
        session = self.session_factory()
        try:
            yield session
            if session.deleted or session.dirty or session.new:
                raise Exception(
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

    def _fetch(self, db_cls, where=None, _reduce=None, _unpack=True):
        with self.make_session() as session:
            query = session.query(*_listify(db_cls))
            if where is not None:
                query = query.filter(where)

            result = list(query.all()) if _reduce is None else _reduce(query)
        if _unpack and not isinstance(db_cls, (tuple, list, DeclarativeMeta)):
            result = [r[0] for r in result]
        return result

    def _fetch_dataframe(self, db_cls, where=None, order_by=None, _reduce=None, chunksize: int = 10000):
        with self.make_session() as session:
            query = session.query(*_listify(db_cls))
            if where is not None:
                query = query.filter(where)
            if order_by is not None:
                query = query.order_by(order_by.desc())

            dfs = []
            compiled_query = query.statement.compile()
            for chunk in pd.read_sql_query(str(compiled_query), con=self.engine, chunksize=chunksize, params=compiled_query.params):
                dfs.append(chunk)

            # Combine all chunks into a single DataFrame
            df = pd.concat(dfs, ignore_index=True)
            return df

    def fetch_all(self, db_cls, where=None):
        return self._fetch(db_cls, where=where, _reduce=lambda q: list(q.all()))

    def fetch_first(self, db_cls, where=None, order_by=None):
        with self.make_session() as session:
            query = session.query(*_listify(db_cls))
            if where is not None:
                query = query.filter(where)
            if order_by is not None:
                query = query.order_by(order_by.desc())

            return query.first()

    def insert(self, db_obj):
        with self.make_writeable_session() as session:
            session.add_all(_listify(db_obj))

    def insert_or_update(self, db_obj):
        with self.make_writeable_session() as session:
            for obj in _listify(db_obj):
                session.merge(obj)


class SlurmMonitorDB(Database):
    Nodes = Nodes
    JobStatus = JobStatus
    GPUStatus = GPUStatus

    def apply_resolution(
            self, data: list[GPUStatus], resolution_in_s: int,
    ) -> list[GPUStatus]:
        smoothed_data = []
        samples_in_window = {}

        base_time = None
        window_start_time = None
        window_index = 0

        for sample in data:
            if not base_time:
                base_time = dt.datetime.fromisoformat(sample.timestamp)
                window_start_time = base_time
                window_index = 0

            if (
                dt.datetime.fromisoformat(sample.timestamp) - window_start_time
            ).total_seconds() < resolution_in_s:
                if sample.uuid not in samples_in_window:
                    samples_in_window[sample.uuid] = [sample]
                else:
                    samples_in_window[sample.uuid].append(sample)
            else:
                smoothed_data.append(GPUStatus.merge(samples_in_window[sample.uuid]))
                window_index += 1

                samples_in_window[sample.uuid] = [sample]
                window_start_time = base_time + dt.timedelta(seconds=window_index*resolution_in_s)


        for uuid, values in samples_in_window.items():
            if values:
                smoothed_data.append(GPUStatus.merge(values))

        return smoothed_data

    def get_gpu_uuids(self, node: str) -> list[str]:
        with self.make_session() as session:
            query = (
                session.query(GPUStatus.uuid).where(GPUStatus.node == node).distinct()
            )
            return [x[0] for x in query.all()]

    def get_nodes(self) -> list[Nodes]:
        return self.fetch_all(Nodes)

    def get_gpu_infos(self, node: str) -> dict[str, any]:
        try:
            gpu_uuids = self.get_gpu_uuids(node=node)
            gpu_infos = [
                    self.fetch_first(GPUStatus,
                        where=((GPUStatus.node == node) & (GPUStatus.uuid == gpu_uuid)),
                        order_by=GPUStatus.timestamp
                    )
                for gpu_uuid in gpu_uuids
            ]
            gpu_info_dicts = [x.to_dict() for x in gpu_infos]
            return { "gpus": gpu_info_dicts }
        except Exception as e:
            logger.warn(e)
            raise

        return {"gpus": []}

    def get_gpu_status(
        self,
        node: str | None = None,
        uuid: str | None = None,
        start_time_in_s: float | None = None,
        end_time_in_s: float | None = None,
        resolution_in_s: int | None = None,
    ) -> list[GPUStatus]:
        where = sqlalchemy.sql.true()
        if node is not None:
            where &= GPUStatus.node == node
            logger.info(f"SlurmMonitorDB.get_gpu_status: {node=}")

        if start_time_in_s is not None:
            start_time = dt.datetime.utcfromtimestamp(start_time_in_s)
            logger.info(f"SlurmMonitorDB.get_gpu_status: {start_time=}")
            where &= GPUStatus.timestamp >= start_time

        if end_time_in_s is not None:
            end_time = dt.datetime.utcfromtimestamp(end_time_in_s)
            logger.info(f"SlurmMonitorDB.get_gpu_status: {end_time=}")
            where &= GPUStatus.timestamp < end_time

        if uuid is not None:
            where &= GPUStatus.uuid == uuid

        data = self.fetch_all(GPUStatus, where=where)
        if resolution_in_s is not None:
            return self.apply_resolution(data=data, resolution_in_s=resolution_in_s)
        else:
            return data

    def get_gpu_status_timeseries_list(
        self,
        nodes: list[str] | None = None,
        start_time_in_s: float | None = None,
        end_time_in_s: float | None = None,
        resolution_in_s: int | None = None,
        local_indices: list[int] | None = None
    ) -> list[dict[str, any]]:
        gpu_status_series_list = []

        logger.info(f"{nodes=} {start_time_in_s=} {end_time_in_s=} {resolution_in_s=} {local_indices=}")
        if nodes is None or nodes == []:
            nodes = self.fetch_all(Nodes.name)

        for node in nodes:
            uuids = self.get_gpu_uuids(node)
            for idx, uuid in enumerate(uuids):
                if local_indices is not None:
                    if idx not in local_indices:
                        continue

                node_data = self.get_gpu_status(
                    node=node,
                    uuid=uuid,
                    start_time_in_s=start_time_in_s,
                    end_time_in_s=end_time_in_s,
                    resolution_in_s=resolution_in_s,
                )

                if node_data:
                    local_id = node_data[0].local_id
                else:
                    local_id = idx
                gpu_status_series = {
                    "label": f"{node}-gpu-{local_id}",
                    "data": node_data,
                }
                gpu_status_series_list.append(gpu_status_series)
        return gpu_status_series_list

    def get_job(self, job_id: int) -> JobStatus:
        where = sqlalchemy.sql.true()
        where &= JobStatus.job_id == job_id

        data = self.fetch_all(JobStatus, where=where)
        if data:
            return data[0]
        else:
            return None


if __name__ == "__main__":
    db_settings = DatabaseSettings(
        user="", password="", uri="sqlite:////tmp/test.sqlite"
    )
    db = SlurmMonitorDB(db_settings=db_settings)

    value = {
        "name": "Tesla V100-SXM3-32GB",
        "uuid": "GPU-ad466f2f-575d-d949-35e0-9a7d912d974e",
        "power.draw": "313.07",
        "temperature.gpu": "52",
        "utilization.gpu": "82",
        "utilization.memory": "28",
        "memory.used": "4205",
        "memory.free": "28295",
    }

    db.insert_or_update(Nodes(name="g001"))

    db.insert(
        GPUStatus(
            name=value["name"],
            uuid=value["uuid"],
            node="g001",
            power_draw=value["power.draw"],
            temperature_gpu=value["temperature.gpu"],
            utilization_memory=value["utilization.memory"],
            utilization_gpu=value["utilization.gpu"],
            memory_used=value["memory.used"],
            memory_free=value["memory.free"],
            timestamp=dt.datetime.now(),
        )
    )

    print(db.fetch_all(GPUStatus))
