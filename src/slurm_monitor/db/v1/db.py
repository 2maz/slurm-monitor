import logging
import sqlalchemy
from contextlib import contextmanager
import datetime as dt
import os

from pydantic import BaseModel
from sqlalchemy import MetaData, event, create_engine
from sqlalchemy.orm import DeclarativeMeta, sessionmaker
from sqlalchemy.engine.url import URL, make_url

from .db_tables import CPUStatus, GPUs, GPUStatus, JobStatus, Nodes, ProcessStatus, TableBase
import pandas as pd
from typing import TypeVar

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
        elif db_url.get_dialect().name == "timescaledb":
            pass

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

            return pd.DataFrame([dict(x) for x in query.all()])

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
    GPUs = GPUs
    JobStatus = JobStatus
    GPUStatus = GPUStatus
    CPUStatus = CPUStatus
    ProcessStatus = ProcessStatus

    T = TypeVar('T')
    # T needs to comply to merge and get_id
    def apply_resolution(
            self, data: list[T], resolution_in_s: int,
    ) -> list[T]:
        smoothed_data = []
        samples_in_window = {}

        base_time = None
        window_start_time = None
        window_index = 0

        if not data:
            return data

        # requiring ordered list (oldest first)
        if data[0].timestamp > data[-1].timestamp:
            data.reverse()

        for sample in data:
            sample_timestamp = sample.timestamp
            if type(sample.timestamp) == str:
                sample_timestamp = dt.datetime.fromisoformat(sample.timestamp)

            if not base_time:
                base_time = sample_timestamp
                window_start_time = base_time
                window_index = 0

            if (
                sample_timestamp - window_start_time
            ).total_seconds() < resolution_in_s:
                if sample.get_id() not in samples_in_window:
                    samples_in_window[sample.get_id()] = [sample]
                else:
                    samples_in_window[sample.get_id()].append(sample)
            else:
                smoothed_data.append(sample.merge(samples_in_window[sample.get_id()]))
                window_index += 1

                samples_in_window[sample.get_id()] = [sample]
                window_start_time = base_time + dt.timedelta(seconds=window_index*resolution_in_s)


        for _, values in samples_in_window.items():
            if values:
                smoothed_data.append(sample.merge(values))

        return smoothed_data

    def get_gpu_uuids(self, node: str) -> list[str]:
        return self.fetch_all(GPUs.uuid, GPUs.node == node)

    def get_nodes(self) -> list[Nodes]:
        return self.fetch_all(Nodes)

    def get_gpu_infos(self, node: str) -> dict[str, any]:
        try:
            gpus = self.fetch_all(GPUs, GPUs.node == node)
            gpu_info_dicts = [dict(x) for x in gpus]
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
            uuids = self.fetch_all(GPUs.uuid, GPUs.node == node)
            where &= GPUStatus.uuid.in_(uuids)

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
                    uuid=uuid,
                    start_time_in_s=start_time_in_s,
                    end_time_in_s=end_time_in_s,
                    resolution_in_s=resolution_in_s,
                )
                result = self.fetch_all(GPUs.local_id, GPUs.uuid == uuid)
                if not result:
                    raise RuntimeError("Failed to retrieve local_id for GPU {uuid=}")

                local_id = result[0]
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

    def get_job_status_timeseries_list(
        self,
        job_id: int,
        start_time_in_s: float | None = None,
        end_time_in_s: float | None = None,
        resolution_in_s: int | None = None,
    ) -> list[dict[str, any]]:
        job_timeseries = []
        logger.info(f"{job_id=} {start_time_in_s=} {end_time_in_s=} {resolution_in_s=}")

        where = (ProcessStatus.job_id == job_id)
        if start_time_in_s is not None:
            start_time = dt.datetime.utcfromtimestamp(start_time_in_s)
            logger.info(f"SlurmMonitorDB.get_job_status_timeseries: {start_time=}")
            where &= ProcessStatus.timestamp >= start_time

        if end_time_in_s is not None:
            end_time = dt.datetime.utcfromtimestamp(end_time_in_s)
            logger.info(f"SlurmMonitorDB.get_job_status_timeseries: {end_time=}")
            where &= ProcessStatus.timestamp < end_time


        with self.make_session() as session:
            pids = session.query(ProcessStatus.pid).filter(where).distinct().all()
            if pids:
                pids = [x[0] for x in pids]

        job_timeseries = {}
        for pid in pids:
            process_status_timeseries = self.fetch_all(ProcessStatus, where=where & (ProcessStatus.pid == pid))

            if resolution_in_s is not None:
                process_status_timeseries = self.apply_resolution(
                        data=process_status_timeseries,
                        resolution_in_s=resolution_in_s
                )

            job_timeseries[pid] = process_status_timeseries

        return job_timeseries



if __name__ == "__main__":
    db_settings = DatabaseSettings(
        user="", password="", uri="timescaledb://slurmuser:test@localhost:7000/ex3cluster"
    )
    db = SlurmMonitorDB(db_settings=db_settings)

    #value = {
    #    "name": "Tesla V100-SXM3-32GB",
    #    "uuid": "GPU-ad466f2f-575d-d949-35e0-9a7d912d974e",
    #    "local_id": 0,
    #    "power.draw": "313.07",
    #    "temperature.gpu": "52",
    #    "utilization.gpu": "82",
    #    "utilization.memory": "28",
    #    "memory.used": "4205",
    #    "memory.free": "28295",
    #}

    #db.insert_or_update(Nodes(
    #    name="g001",
    #    gpu_count=16,
    #    gpu_model="Tesla",
    #    cpu_count="128",
    #    cpu_model="Epyc"
    #    )
    #)

    #db.insert(
    #    GPUStatus(
    #        name=value["name"],
    #        uuid=value["uuid"],
    #        node="g001",
    #        local_id=value["local_id"],
    #        power_draw=value["power.draw"],
    #        temperature_gpu=value["temperature.gpu"],
    #        utilization_memory=value["utilization.memory"],
    #        utilization_gpu=value["utilization.gpu"],
    #        memory_total=int(value["memory.used"]) + int(value["memory.free"]),
    #        timestamp=dt.datetime.now(),
    #    )
    #)

    print(db.first(GPUStatus.name, GPUStatus.timestamp))
