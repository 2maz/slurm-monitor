import asyncio
from collections.abc import Awaitable
from contextlib import contextmanager, asynccontextmanager
import datetime as dt
import logging
import shutil

import os
from pathlib import Path
from pydantic import BaseModel
import sqlalchemy
from sqlalchemy import (
        MetaData,
        event,
        create_engine,
        func,
        select,
)
from sqlalchemy.orm import DeclarativeMeta, sessionmaker
from sqlalchemy.engine.url import URL, make_url
from sqlalchemy.ext.asyncio import (
        create_async_engine,
        async_sessionmaker,
        AsyncEngine,
        AsyncSession
)

from .db_tables import (
        EpochFn,
        CPUStatus,
        GPUs,
        GPUProcess,
        GPUProcessStatus,
        GPUStatus,
        LocalIndexedGPUs,
        JobStatus,
        Nodes,
        MemoryStatus,
        ProcessStatus,
        TableBase
)
import pandas as pd
import tempfile
from tqdm import tqdm
from zipfile import ZipFile

from slurm_monitor.utils import (
    utcnow,
    ensure_utc
)

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
    async_engine: AsyncEngine

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

        async_db_url = db_url
        if db_settings.uri.startswith("sqlite://"):
            async_db_url = create_url(
                    db_settings.uri.replace("sqlite:","sqlite+aiosqlite:"),
                    db_settings.user,
                    db_settings.password
            )
        elif db_settings.uri.startswith("timescaledb://"):
            async_db_url = create_url(
                    db_settings.uri.replace("timescaledb:","timescaledb+asyncpg:"),
                    db_settings.user,
                    db_settings.password
            )

        self.async_engine = create_async_engine(async_db_url, **engine_kwargs)
        self.async_session_factory = async_sessionmaker(self.async_engine, expire_on_commit=False)

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

    def _fetch(self, db_cls, where=None, _reduce=None, _unpack=True, limit: int | None = None):
        with self.make_session() as session:
            query = session.query(*_listify(db_cls))
            if where is not None:
                query = query.filter(where)

            if limit:
                query = query.limit(limit)

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

    def fetch_all(self, db_cls, where=None, limit: int | None = None):
        return self._fetch(db_cls, where=where, limit=limit)

    def fetch_first(self, db_cls, where=None, order_by=None):
        with self.make_session() as session:
            query = session.query(*_listify(db_cls))
            if where is not None:
                query = query.filter(where)
            if order_by is not None:
                query = query.order_by(order_by)

            return query.first()

    def fetch_latest(self, db_cls, where=None):
        return self.fetch_first(db_cls=db_cls, where=where, order_by=db_cls.timestamp.desc())

    def insert(self, db_obj):
        with self.make_writeable_session() as session:
            session.add_all(_listify(db_obj))

    def insert_or_update(self, db_obj):
        with self.make_writeable_session() as session:
            for obj in _listify(db_obj):
                session.merge(obj)
# async ----------------------------------------------------------
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
            query = query.filter(where)
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
       return await self.fetch_first_async(db_cls=db_cls, where=where, order_by=db_cls.timestamp.desc())


class SlurmMonitorDB(Database):
    Nodes = Nodes

    CPUStatus = CPUStatus
    MemoryStatus = MemoryStatus
    ProcessStatus = ProcessStatus

    GPUs = GPUs
    GPUStatus = GPUStatus
    GPUProcess = GPUProcess
    GPUProcessStatus = GPUProcessStatus

    LocalIndexedGPUs = LocalIndexedGPUs

    JobStatus = JobStatus

    def get_nodes(self) -> list[str]:
        return list(set(self.fetch_all(Nodes.name)))

    async def get_last_probe_timestamp(self) -> Awaitable[dict[str, float]]:
        nodes = self.get_nodes()
        tasks = {}
        for node in nodes:
            tasks[node] = asyncio.create_task(self.fetch_latest_async(CPUStatus, where=(CPUStatus.node == node)))
        return {node : (await tasks[node]).timestamp for node in nodes}

    def get_gpu_uuids(self, node: str, when: dt.datetime | None = dt.datetime.now(dt.timezone.utc)) -> list[str]:
        where = (LocalIndexedGPUs.node == node)
        if when is not None:
            where &= (LocalIndexedGPUs.start_time <= when) & (LocalIndexedGPUs.end_time > when)

        return self.fetch_all(LocalIndexedGPUs.uuid,
                where=where
        )

    def get_gpu_local_id(self, uuid: str, when: dt.datetime | None = dt.datetime.now(dt.timezone.utc)) -> list[str]:
        where = (LocalIndexedGPUs.uuid == uuid)
        if when is not None:
            where &= (LocalIndexedGPUs.start_time <= when) & (LocalIndexedGPUs.end_time > when)

        return self.fetch_all(LocalIndexedGPUs.local_id,
                where=where
        )

    def get_gpu_uuids_from_local_ids(self, node: str,
            local_ids: list[int] = [],
            when: dt.datetime | None = dt.datetime.now(dt.timezone.utc)) -> list[str]:
        where = (LocalIndexedGPUs.local_id.in_(local_ids) ) & (LocalIndexedGPUs.node == node)
        if when is not None:
            where &= (LocalIndexedGPUs.start_time <= when) & (LocalIndexedGPUs.end_time > when)

        return self.fetch_all(LocalIndexedGPUs.uuid,
                where=where)

    def get_gpu_nodes(self) -> list[str]:
        return list(set(self.fetch_all(GPUs.node)))

    def get_cpu_infos(self, node: str) -> dict[str, any]:
        try:
            cpu_infos = self.fetch_all([Nodes.cpu_count, Nodes.cpu_model], Nodes.name == node)
            if cpu_infos:
                count, model = cpu_infos[0]
                return { 'cpus': { 'model': model, 'count': count } }
        except Exception as e:
            logger.warn(e)
            raise

        return {"cpus": {}}

    def get_gpu_infos(self, node: str) -> dict[str, any]:
        try:
            gpus = self.fetch_all(GPUs, GPUs.node == node)
            gpu_info_dicts = [dict(x) for x in gpus]
            return { "gpus": gpu_info_dicts }
        except Exception as e:
            logger.warn(e)
            raise

        return {"gpus": []}

    async def get_gpu_status(
        self,
        node: str | None = None,
        uuid: str | None = None,
        start_time_in_s: float | None = None,
        end_time_in_s: float | None = None,
        resolution_in_s: int | None = None,
    ) -> list[GPUStatus]:
        where = sqlalchemy.sql.true()
        if node is not None:
            uuids = await self.fetch_all_async(GPUs.uuid, GPUs.node == node)
            where &= GPUStatus.uuid.in_(uuids)

        start_time = None
        if start_time_in_s is not None:
            start_time = dt.datetime.fromtimestamp(start_time_in_s, dt.timezone.utc).replace(tzinfo=None)
            where &= GPUStatus.timestamp >= start_time

        end_time = None
        if end_time_in_s is not None:
            end_time = dt.datetime.fromtimestamp(end_time_in_s, dt.timezone.utc).replace(tzinfo=None)
            where &= GPUStatus.timestamp <= end_time

        if uuid is not None:
            where &= GPUStatus.uuid == uuid

        logger.info(f"SlurmMonitorDB.get_gpu_status: {node=} {uuid=} {start_time=} {end_time=} {resolution_in_s=}")
        data = await self.fetch_all_async(GPUStatus, where=where, order_by=(GPUStatus.timestamp.desc()))
        if resolution_in_s is not None:
            return TableBase.apply_resolution(data=data, resolution_in_s=resolution_in_s)
        else:
            return data

    async def get_gpu_status_timeseries_list(
        self,
        nodes: list[str] | None = None,
        start_time_in_s: float | None = None,
        end_time_in_s: float | None = None,
        resolution_in_s: int | None = None,
        local_indices: list[int] | None = None
    ) -> list[dict[str, any]]:

        logger.info(f"{nodes=} {start_time_in_s=} {end_time_in_s=} {resolution_in_s=} {local_indices=}")
        if nodes is None or nodes == []:
            nodes = await self.fetch_all_async(Nodes.name)

        uuid2node = {}
        for node in nodes:
            when = None
            if start_time_in_s is not None:
                when = dt.datetime.fromtimestamp(start_time_in_s, dt.timezone.utc).replace(tzinfo=None)

            uuids = self.get_gpu_uuids(node, when=when)
            if local_indices is None:
                local_indices = range(0, len(uuids))

            for local_id in local_indices:
                result = await self.fetch_all_async(GPUs.uuid, (GPUs.local_id == local_id) & (GPUs.node == node))
                if not result:
                    raise RuntimeError(
                            "SlurmMonitorDB.get_gpu_status_timeseries_list:"
                            f"Failed to retrieve uuid for GPU {local_id=} on {node=}"
                    )

                uuid = result[0]
                uuid2node[uuid] = { 'node': node, 'local_id': local_id }

        tasks = {}
        for uuid, node in uuid2node.items():
            tasks[uuid] = asyncio.create_task(self.get_gpu_status(
                uuid=uuid,
                start_time_in_s=start_time_in_s,
                end_time_in_s=end_time_in_s,
                resolution_in_s=resolution_in_s,
            ))

        return [{
            "label": f"{uuid2node[uuid]['node']}-gpu-{uuid2node[uuid]['local_id']}",
            "data": await tasks[uuid],
        } for uuid in tasks.keys()]

    async def get_cpu_status(
        self,
        node: str,
        start_time_in_s: float | None = None,
        end_time_in_s: float | None = None,
        resolution_in_s: int | None = None,
    ) -> list[GPUStatus]:
        where = CPUStatus.node == node

        start_time = None
        if start_time_in_s is not None:
            start_time = dt.datetime.fromtimestamp(start_time_in_s, dt.timezone.utc).replace(tzinfo=None)
            where &= CPUStatus.timestamp >= start_time

        end_time = None
        if end_time_in_s is not None:
            end_time = dt.datetime.fromtimestamp(end_time_in_s, dt.timezone.utc).replace(tzinfo=None)
            where &= CPUStatus.timestamp < end_time

        logger.info(f"SlurmMonitorDB.get_cpu_status: {node=} {start_time=} {end_time=} {resolution_in_s=}")
        data = await self.fetch_all_async(CPUStatus, where=where, order_by=CPUStatus.timestamp.desc())
        if resolution_in_s is not None:
            return TableBase.apply_resolution(data=data, resolution_in_s=resolution_in_s)
        else:
            return data

    async def get_cpu_status_timeseries_list(
        self,
        nodes: list[str] | None = None,
        start_time_in_s: float | None = None,
        end_time_in_s: float | None = None,
        resolution_in_s: int | None = None,
    ) -> list[dict[str, any]]:

        logger.info(f"{nodes=} {start_time_in_s=} {end_time_in_s=} {resolution_in_s=}")
        if nodes is None or nodes == []:
            nodes = await self.fetch_all_async(Nodes.name)

        tasks = {}
        for node in nodes:
            tasks[node] = asyncio.create_task(self.get_cpu_status(
                node=node,
                start_time_in_s=start_time_in_s,
                end_time_in_s=end_time_in_s,
                resolution_in_s=resolution_in_s,
            ))

        # cpu_status_series_list
        return [{
                "label": f"{node}-cpu",
                "data": await tasks[node]
            } for node in nodes]

    async def get_memory_status(
        self,
        node: str | None = None,
        start_time_in_s: float | None = None,
        end_time_in_s: float | None = None,
        resolution_in_s: int | None = None,
        ) -> Awaitable[list[GPUStatus]]:
        where = sqlalchemy.sql.true()

        if node is not None:
            where &= MemoryStatus.node  == node

        start_time = None
        if start_time_in_s is not None:
            start_time = dt.datetime.fromtimestamp(start_time_in_s, dt.timezone.utc).replace(tzinfo=None)
            where &= MemoryStatus.timestamp >= start_time

        end_time = None
        if end_time_in_s is not None:
            end_time = dt.datetime.fromtimestamp(end_time_in_s, dt.timezone.utc).replace(tzinfo=None)
            where &= MemoryStatus.timestamp <= end_time

        logger.info(f"SlurmMonitorDB.get_memory_status: {node=} {start_time=} {end_time=} {resolution_in_s=}")
        data = await self.fetch_all_async(MemoryStatus, where=where, order_by=MemoryStatus.timestamp.desc())
        if resolution_in_s is not None:
            return TableBase.apply_resolution(data=data, resolution_in_s=resolution_in_s)
        else:
            return data

    async def get_memory_status_timeseries_list(
        self,
        nodes: list[str] | None = None,
        start_time_in_s: float | None = None,
        end_time_in_s: float | None = None,
        resolution_in_s: int | None = None,
        ) -> Awaitable[list[dict[str, any]]]:

        logger.info(f"{nodes=} {start_time_in_s=} {end_time_in_s=} {resolution_in_s=}")
        if nodes is None or nodes == []:
            nodes = await self.fetch_all_async(Nodes.name)

        tasks = {}
        for node in nodes:
            tasks[node] = asyncio.create_task(self.get_memory_status(
                node=node,
                start_time_in_s=start_time_in_s,
                end_time_in_s=end_time_in_s,
                resolution_in_s=resolution_in_s,
            ))

        # memory_status_series_list
        return [{
                "label": f"{node}-memory",
                "data": await tasks[node]
            } for node in nodes]

    async def get_job(self, job_id: int) -> JobStatus | None:
        where = sqlalchemy.sql.true()
        where &= JobStatus.job_id == job_id

        data = await self.fetch_all_async(JobStatus, where=where)
        if data:
            return data[0]
        else:
            return None

    async def get_jobs(self,
            user: str | None = None,
            user_id: int | None = None,
            job_id: int | None = None,
            start_before_in_s: float | None = None,
            start_after_in_s: float | None = None,
            end_before_in_s: float | None = None,
            end_after_in_s: float | None = None,
            submit_before_in_s: float | None = None,
            submit_after_in_s: float | None = None,
            min_duration_in_s: float | None = None,
            max_duration_in_s: float | None = None,
            limit: int = 100
        ):

        where = sqlalchemy.sql.true()
        if user:
            where &= JobStatus.user_name == user

        if user_id:
            where &= JobStatus.user_id == user_id

        if job_id:
            where &= JobStatus.job_id == job_id

        if start_before_in_s is not None:
            reference_time = dt.datetime.fromtimestamp(start_before_in_s, dt.timezone.utc).replace(tzinfo=None)
            where &= JobStatus.start_time <= reference_time

        if start_after_in_s is not None:
            reference_time = dt.datetime.fromtimestamp(start_after_in_s, dt.timezone.utc).replace(tzinfo=None)
            where &= JobStatus.start_time >= reference_time

        if end_before_in_s is not None:
            reference_time = dt.datetime.fromtimestamp(end_before_in_s, dt.timezone.utc).replace(tzinfo=None)
            where &= JobStatus.end_time <= reference_time

        if end_after_in_s is not None:
            reference_time = dt.datetime.fromtimestamp(end_after_in_s, dt.timezone.utc).replace(tzinfo=None)
            where &= JobStatus.end_time >= reference_time

        if submit_before_in_s is not None:
            reference_time = dt.datetime.fromtimestamp(submit_before_in_s, dt.timezone.utc).replace(tzinfo=None)
            where &= JobStatus.submit_time <= reference_time

        if submit_after_in_s is not None:
            reference_time = dt.datetime.fromtimestamp(submit_after_in_s, dt.timezone.utc).replace(tzinfo=None)
            where &= JobStatus.submit_time >= reference_time

        if min_duration_in_s is not None:
            where &= (EpochFn(JobStatus.end_time) - EpochFn(JobStatus.start_time)) >= min_duration_in_s

        if max_duration_in_s is not None:
            where &= (EpochFn(JobStatus.end_time) - EpochFn(JobStatus.start_time)) <= max_duration_in_s

        # limit search to completed jobs
        where &= JobStatus.job_state == 'COMPLETED'
        return await self.fetch_all_async(JobStatus, where=where, limit=limit)


    async def get_process_status(self,
            node: str,
            pid: int,
            resolution_in_s: int | None,
            where):
        process_status_timeseries = await self.fetch_all_async(ProcessStatus,
                where=where
                & (ProcessStatus.pid == pid)
                & (ProcessStatus.node == node)
        )

        if resolution_in_s is not None:
            process_status_timeseries = TableBase.apply_resolution(
                    data=process_status_timeseries,
                    resolution_in_s=resolution_in_s
            )
        return process_status_timeseries

    async def get_accumulated_process_status(self,
            node: str,
            job_id: int,
            resolution_in_s: int,
            where
            ):
            # accumulated timeseries
            query = (
                     select(ProcessStatus.node,
                            ProcessStatus.timestamp,
                            func.sum(ProcessStatus.cpu_percent).label('cpu_sum'),
                            func.sum(ProcessStatus.memory_percent).label('memory_sum')
                     )
                     .where(where & (ProcessStatus.node == node))
                     .group_by(
                         ProcessStatus.node,
                         ProcessStatus.job_id,
                         ProcessStatus.timestamp
                     )
                     .order_by(ProcessStatus.timestamp.desc())
            )

            async with self.make_async_session() as session:
                accumulated_timeseries = [ProcessStatus(
                    job_id=job_id,
                    pid=0, # dummy
                    node=x[0],
                    timestamp=x[1],
                    cpu_percent=x[2],
                    memory_percent=x[3]
                    )
                    for x in (await session.execute(query)).all()
                ]

                if resolution_in_s is not None:
                    accumulated_timeseries = TableBase.apply_resolution(
                            data=accumulated_timeseries,
                            resolution_in_s=resolution_in_s
                    )

                return accumulated_timeseries

    async def get_gpu_process_status(self,
            node: str,
            job_id: int,
            resolution_in_s: int,
            where
            ):
            job_status = await self.get_job(job_id=job_id)
            uuids = self.get_gpu_uuids_from_local_ids(
                    node=node,
                    local_ids=job_status.gres_detail,
                    when=job_status.start_time
            )

            # accumulated timeseries
            query = (
                     select(GPUProcessStatus.uuid,
                            GPUProcessStatus.timestamp,
                            GPUProcessStatus.utilization_sm,
                            GPUProcessStatus.used_memory,
                     )
                     .where(where & (GPUProcessStatus.uuid.in_(uuids)))
                     .order_by(GPUProcessStatus.timestamp.desc())
            )

            async with self.make_async_session() as session:
                return [
                        GPUProcessStatus(
                            uuid=x[0],
                            timestamp=x[1],
                            utilization_sm=x[2], # in percent
                            used_memory=x[3] # in bytes
                        )
                        for x in (await session.execute(query)).all()
                ]

    async def get_accumulated_gpu_process_status(self,
            node: str,
            job_id: int,
            resolution_in_s: int,
            where
            ):

            job_status = await self.get_job(job_id=job_id)
            uuids = self.get_gpu_uuids_from_local_ids(
                    node=node,
                    local_ids=job_status.gres_detail,
                    when=job_status.start_time
            )
            query = select(func.sum(GPUs.memory_total).label('total_memory')) \
                    .where(GPUs.uuid.in_(uuids))

            async with self.make_async_session() as session:
                total_memory = float((await session.execute(query)).all()[0][0])

            # accumulated timeseries
            query = (
                     select(GPUProcessStatus.uuid,
                            GPUProcessStatus.timestamp,
                            func.sum(GPUProcessStatus.utilization_sm).label('utilization_sm'),
                            func.sum(GPUProcessStatus.used_memory).label('used_memory')
                     )
                     .where(where & (GPUProcessStatus.uuid.in_(uuids)))
                     .group_by(
                         GPUProcessStatus.uuid,
                         GPUProcessStatus.pid,
                         GPUProcessStatus.timestamp
                     )
                     .order_by(GPUProcessStatus.timestamp.desc())
            )

            async with self.make_async_session() as session:
                accumulated_timeseries = [GPUProcessStatus(
                    uuid=0, # dummy
                    timestamp=x[1],
                    utilization_sm=x[2],
                    used_memory=x[3]*100.0 / total_memory
                    )
                    for x in (await session.execute(query)).all()
                ]

                if resolution_in_s is not None:
                    accumulated_timeseries = TableBase.apply_resolution(
                            data=accumulated_timeseries,
                            resolution_in_s=resolution_in_s
                    )

                return accumulated_timeseries

    async def get_active_pids(self,
            node: str,
            job_id: int,
            end_time: dt.datetime = utcnow()):
            reference_time = ensure_utc(end_time)
            active_time_window = reference_time - dt.timedelta(seconds=15*60)
            query = select(ProcessStatus.pid).filter(
                    (ProcessStatus.node == node) &
                    (ProcessStatus.job_id == job_id) &
                    (ProcessStatus.timestamp > active_time_window)
                    ).distinct()

            async with self.make_async_session() as session:
                active_pids = (await session.execute(query)).all()
                if active_pids:
                    active_pids = [x[0] for x in active_pids]

                return active_pids

    async def get_job_status_timeseries_list(
        self,
        job_id: int,
        start_time_in_s: float | None = None,
        end_time_in_s: float | None = None,
        resolution_in_s: int | None = None,
        detailed: bool = False
    ) -> list[dict[str, any]]:
        logger.info(f"{job_id=} {start_time_in_s=} {end_time_in_s=} {resolution_in_s=}")

        where = (ProcessStatus.job_id == job_id)
        if start_time_in_s is not None:
            start_time = dt.datetime.fromtimestamp(start_time_in_s, dt.timezone.utc).replace(tzinfo=None)
            logger.info(f"SlurmMonitorDB.get_job_status_timeseries: {start_time=}")
            where &= ProcessStatus.timestamp >= start_time

        if end_time_in_s is not None:
            end_time = dt.datetime.fromtimestamp(end_time_in_s, dt.timezone.utc).replace(tzinfo=None)
            logger.info(f"SlurmMonitorDB.get_job_status_timeseries: {end_time=}")
            where &= ProcessStatus.timestamp < end_time

        nodes = {}
        with self.make_session() as session:
            result = session.query(ProcessStatus.pid, ProcessStatus.node).filter(where).distinct().all()
            if result:
                nodes = { x[1] : [] for x in result }
                [ nodes[x[1]].append(x[0]) for x in result ]

        now = utcnow()
        timeseries_per_node = {}
        for node, pids in nodes.items():
            accumulated_timeseries_task = asyncio.create_task(
                    self.get_accumulated_process_status(
                        node=node,
                        job_id=job_id,
                        resolution_in_s=resolution_in_s,
                        where=where
                    )
            )

            active_pids_task = asyncio.create_task(
                    self.get_active_pids(
                        node=node,
                        job_id=job_id
                    )
            )

            pid_timeseries_tasks = {}
            if detailed:
                for pid in tqdm(pids):
                    pid_timeseries_tasks[pid] = asyncio.create_task(
                            self.get_process_status(
                                node=node,
                                pid=pid,
                                resolution_in_s=resolution_in_s,
                                where=where
                            )
                    )

            timeseries_per_node[node] = {
                    "pids": pids,
                    "active_pids": await active_pids_task,
                    "accumulated": await accumulated_timeseries_task,
                    "timeseries": { x: await pid_timeseries_tasks[x]
                        for x in pid_timeseries_tasks },
                    "timestamp": now
            }
        return timeseries_per_node

    @classmethod
    def get_export_data_dirname(cls, job_id: int, base_dir: str | Path | None = None) -> Path:
        if base_dir is None:
            export_dir = Path(tempfile.gettempdir()) / "slurm-monitor-export"
        else:
            export_dir = base_dir

        return Path(export_dir) / f"job-{job_id}"

    async def export_data(self, job_id: int, base_dir: str | Path | None = None):
        job = self.fetch_all(JobStatus,
                (JobStatus.job_id == job_id)
              )[0]
        # single vs. multi-node job
        node = job.nodes
        node_info = self.fetch_all(Nodes, Nodes.name == node)

        uuids = []
        for local_id in job.gres_detail:
            uuid = self.fetch_all(GPUs.uuid,
                    (GPUs.local_id == local_id)
                    & (GPUs.node == node)
            )
            uuids.append(uuid[0])

        if uuids:
            gpus = self.fetch_all(GPUs,
                GPUs.uuid.in_(uuids)
            )

            self.fetch_all(GPUProcess,
                (GPUProcess.job_id == job.job_id)
            )

            gpu_process_status_accumulated_data = await self.get_accumulated_gpu_process_status(node=node,
                job_id=job.job_id,
                resolution_in_s=None,
                where=(GPUProcessStatus.timestamp <= job.end_time) & (GPUProcessStatus.timestamp >= job.start_time)
            )
            gpu_process_status_data = await self.get_gpu_process_status(node=node,
                job_id=job.job_id,
                resolution_in_s=None,
                where=(GPUProcessStatus.timestamp <= job.end_time) & (GPUProcessStatus.timestamp >= job.start_time)
            )

        process_status_data = await self.get_accumulated_process_status(node=node,
            job_id=job.job_id,
            resolution_in_s=None,
            where=(ProcessStatus.timestamp <= job.end_time) & (ProcessStatus.timestamp >= job.start_time)
        )

        parent = self.get_export_data_dirname(job_id=job_id, base_dir=base_dir)
        if parent.exists():
            shutil.rmtree(parent)

        parent.mkdir(parents=True, exist_ok=True)

        df = pd.DataFrame([dict(x) for x in process_status_data])
        df.to_parquet(parent / "processes.parquet")

        if uuids:
            gpu_process_status_data.reverse()
            df = pd.DataFrame([dict(x) for x in gpu_process_status_data])
            df.to_parquet(parent / "gpu_process_status.parquet")

            gpu_process_status_accumulated_data.reverse()
            df = pd.DataFrame([dict(x) for x in gpu_process_status_accumulated_data])
            df.to_parquet(parent / "gpu_process_status-accmulated.parquet")

            df = pd.DataFrame([dict(x) for x in gpus])
            df.to_parquet(parent / "gpus.parquet")

        # anonimization needed
        df = pd.DataFrame([dict(job)])
        df.to_parquet(parent / "job.parquet")

        df = pd.DataFrame([dict(x) for x in node_info])
        df.to_parquet(parent / "nodes.parquet")

        zip_filename = f"{parent}.zip"

        os.chdir(str(Path(zip_filename).parent))
        with ZipFile(zip_filename, "w") as z:
            for x in Path().glob(f"*-{job_id}/*.parquet"):
                z.write(x)

        return zip_filename

    def clear(self):
        with self.make_writeable_session() as session:
            for table in reversed(self._metadata.sorted_tables):
                session.query(table).delete()
