import asyncio
from tqdm import tqdm
import os
from collections.abc import Awaitable
import datetime as dt
from pydantic import BaseModel
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager, asynccontextmanager
from sqlalchemy.engine.url import URL, make_url
from sqlalchemy import (
        MetaData,
        event,
        create_engine,
        func,
        select,
        column,
        over
)
from sqlalchemy.ext.asyncio import (
        create_async_engine,
        async_sessionmaker,
        AsyncEngine,
        AsyncSession
)

import logging
from slurm_monitor.utils import utcnow

logger = logging.getLogger(__name__)


from .db_tables import (
    GPUCard,
    GPUCardConfig,
    GPUCardStatus,
    GPUCardProcessStatus,
    Node,
    NodeConfig,
    ProcessStatus,
    SoftwareVersion,
    TableMetadata,
    TableBase
)

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

        self.async_engine = create_async_engine(async_db_url, **engine_kwargs)
        self.async_session_factory = async_sessionmaker(self.async_engine, expire_on_commit=False)

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



    def insert(self, db_obj):
        with self.make_writeable_session() as session:
            session.add_all(_listify(db_obj))

    def insert_or_update(self, db_obj):
        with self.make_writeable_session() as session:
            for obj in _listify(db_obj):
                session.merge(obj)

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


class ClusterDB(Database):
    Node = Node
    NodeConfig = NodeConfig
    GPUCard = GPUCard
    GPUCardConfig= GPUCardConfig
    GPUCardStatus = GPUCardStatus
    GPUCardProcessStatus = GPUCardProcessStatus

    ProcessStatus = ProcessStatus
    SoftwareVersion = SoftwareVersion
    #TableMetadata = TableMetadata

    def fake_timeseries(self,
            start_time: dt.datetime | None = None,
            to_time: dt.datetime | None = None,
            sampling_interval_in_s = 60
        ):
        self.fake_timeseries_process(
                start_time=start_time,
                to_time=to_time,
                sampling_interval_in_s=sampling_interval_in_s)

        self.fake_timeseries_gpu_process(
                start_time=start_time,
                to_time=to_time,
                sampling_interval_in_s=sampling_interval_in_s)

    def fake_timeseries_process(self,
            start_time: dt.datetime | None = None,
            to_time: dt.datetime | None = None,
            sampling_interval_in_s = 60
        ):

        # Continue process data
        query = select(
                    ProcessStatus.job,
                    ProcessStatus.epoch,
                    ProcessStatus.pid,
                    func.max(ProcessStatus.timestamp),
                ).group_by(
                    ProcessStatus.job,
                    ProcessStatus.epoch,
                    ProcessStatus.pid
                )

        with self.make_writeable_session() as session:
            jobs = session.execute(query).all()

        if to_time is None:
            to_time = utcnow()

        for job in tqdm(jobs, desc="jobs"):
            job, epoch, pid, timestamp = job

            query = select(
                        ProcessStatus
                    ).where(
                        ((ProcessStatus.timestamp) == timestamp) &
                        (ProcessStatus.job == job) &
                        (ProcessStatus.epoch == epoch) &
                        (ProcessStatus.pid == pid)
                    )

            with self.make_writeable_session() as session:
                processes = session.execute(query).all()

            timeseries = []
            samples_to_add = int((to_time - timestamp).total_seconds()/ sampling_interval_in_s)
            for i in tqdm(range(0, samples_to_add), desc=f"samples for {job=} {epoch=} {pid=}"):
                timestamp += dt.timedelta(seconds=sampling_interval_in_s)
                for process in processes:
                    sample = dict(process[0])
                    sample["timestamp"] = timestamp

                    timeseries.append( ProcessStatus(**sample) )

            logger.info(f"Inserting {len(timeseries)} samples for {job=} {epoch=} {pid=}")
            self.insert(timeseries)



    def fake_timeseries_gpu_process(self,
            start_time: dt.datetime | None = None,
            to_time: dt.datetime | None = None,
            sampling_interval_in_s = 60,
        ) -> list[str]:

        # Continue GPU data
        query = select(
                    GPUCardProcessStatus.uuid,
                    func.max(GPUCardProcessStatus.timestamp)
                ).group_by(
                    GPUCardProcessStatus.uuid
                )

        with self.make_writeable_session() as session:
            gpus = session.execute(query).all()

        if to_time is None:
            to_time = utcnow()

        for gpu in tqdm(gpus, desc="gpus"):
            uuid, timestamp = gpu

            query = select(
                        GPUCardProcessStatus
                    ).where(
                        ((GPUCardProcessStatus.timestamp) == timestamp) &
                        (GPUCardProcessStatus.uuid == uuid)
                    )
            with self.make_writeable_session() as session:
                processes = session.execute(query).all()

            timeseries = []
            samples_to_add = int((to_time - timestamp).total_seconds()/ sampling_interval_in_s)
            for i in tqdm(range(0, samples_to_add), desc=f"samples for {uuid}"):
                timestamp += dt.timedelta(seconds=sampling_interval_in_s)
                for process in processes:
                    sample = dict(process[0])
                    sample["timestamp"] = timestamp

                    timeseries.append( GPUCardProcessStatus(**sample) )

            logger.info(f"Inserting {len(timeseries)} samples for {uuid}")
            self.insert(timeseries)


    async def get_nodes(self, cluster: str) -> list[str]:
        query = select(Node.node).filter(
                    Node.cluster == cluster
                ).distinct()

        async with self.make_async_session() as session:
            nodes = (await session.execute(query)).all()
            if nodes:
                nodes = [x[0] for x in nodes]
            return nodes

    async def get_gpu_nodes(self, cluster: str) -> list[str]:
        # SELECT node FROM (SELECT node, max(timestamp) as max, cards FROM node_config GROUP BY node, cards) WHERE cards != '{}';
        subquery = select(NodeConfig.node, NodeConfig.cluster, func.max(NodeConfig.timestamp), NodeConfig.cards).filter(
                        NodeConfig.cluster == cluster
                    ).group_by(NodeConfig.node, NodeConfig.cluster, NodeConfig.cards).subquery()

        query = select(subquery.c.node).select_from(subquery).filter(
                    # https://www.postgresql.org/docs/17/functions-array.html#ARRAY-FUNCTIONS-TABLE
                    # 1 -> dimension
                    func.array_length(subquery.c.cards,1) != 0
                )

        async with self.make_async_session() as session:
            result = (await session.execute(query)).all()
            if result:
                result = [x[0] for x in result]
            return result


    async def get_nodes_info(self, cluster: str, nodelist: list[str] | str | None = None):
        if not nodelist:
            nodelist = await self.get_nodes(cluster=cluster)
        elif type(nodelist) == str:
            nodelist = [nodelist]

        node_configs = await self.fetch_all_async(NodeConfig,
                    (NodeConfig.cluster == cluster) & (NodeConfig.node.in_(nodelist)),
                    order_by=NodeConfig.node
                )

        nodeinfo = {}
        for node_config in node_configs:
            nodename = node_config.node

            nodeinfo[nodename] = dict(node_config)
            if node_config.cards:
                try:
                    nodeinfo[nodename].update({'cards': await self.get_gpu_infos(cluster=cluster, node=nodename)})
                except Exception as e:
                    logger.warn(f"Internal error: Retrieving GPU info for {nodename} failed -- {e}")
        return nodeinfo

    async def get_last_node_configs(self, cluster: str, node: str | list[str] | None = None,
            start_time_in_s: int | None = None ,
            end_time_in_s: int | None = None ) -> NodeConfig:

        nodelist = node
        if node is None:
            nodelist = await self.get_nodes(cluster=cluster)
        elif type(node) == str:
            node = [node]

        try:

            # Find the lastest configuration in a particular timewindow
            subquery = select(
                         NodeConfig.cluster,
                         NodeConfig.node,
                         func.max(NodeConfig.timestamp).label('max_timestamp')
                       ).filter(
                         NodeConfig.cluster == cluster
                       ).filter(
                         NodeConfig.node.in_(node)
                       )

            if start_time_in_s:
                subquery = subquery.filter(
                            NodeConfig.timestamp >= dt.datetime.fromtimestamp(start_time_in_s, dt.timezone.utc)

                        )
            if end_time_in_s:
                subquery = subquery.filter(
                            NodeConfig.timestamp <= dt.datetime.fromtimestamp(end_time_in_s, dt.timezone.utc)
                        )

            subquery = subquery.group_by(
                         NodeConfig.cluster,
                         NodeConfig.node
                       ).subquery()

            query = select(
                        NodeConfig
                    ).join(
                        subquery,
                        (NodeConfig.cluster == subquery.c.cluster) &
                        (NodeConfig.node == subquery.c.node) &
                        (NodeConfig.timestamp == subquery.c.max_timestamp)
                    ).order_by(None)


            async with self.make_async_session() as session:
                return [x[0] for x in (await session.execute(query)).all()]

        except Exception as e:
            logger.warning(e)
            raise

        return []


    async def get_gpu_infos(self, cluster: str, node: str,
            start_time_in_s: int | None = None ,
            end_time_in_s: int | None = None ) -> Awaitable[list[dict[str, any]]]:
        """
        Get an array of GPUCard + GPUCardConfig information for the given cluster and node
        """
        # model, node, uuid, local_id, memory_total
        try:

            node_config = await self.get_last_node_configs(
                                    cluster=cluster, node=node,
                                    start_time_in_s=start_time_in_s,
                                    end_time_in_s=end_time_in_s
                            )

            if node_config:
                node_config = node_config[0]
                cards = [x[0] for x in node_config.cards]
            else:
                raise RuntimeError(f"Failed to retrieve node config for {cluster=} {node=}")

            # Subquery to get the latest GPUCardConfig within a timewindow
            subquery = select(
                            GPUCardConfig.uuid,
                            func.max(GPUCardConfig.timestamp).label('max_timestamp')
                       ).filter(
                            GPUCardConfig.uuid.in_(cards)
                       )

            if start_time_in_s:
                subquery = subquery.filter(
                            GPUCardConfig.timestamp >= dt.datetime.fromtimestamp(start_time_in_s, dt.timezone.utc)
                        )
            if end_time_in_s:
                subquery = subquery.filter(
                            GPUCardConfig.timestamp <= dt.datetime.fromtimestamp(end_time_in_s, dt.timezone.utc)
                        )

            subquery = subquery.group_by(
                            GPUCardConfig.uuid
                       ).subquery()

            query = select(
                        GPUCard,
                        GPUCardConfig
                    ).select_from(GPUCard).join(
                        GPUCardConfig, GPUCard.uuid == GPUCardConfig.uuid
                    ).join(
                        subquery,
                        (GPUCard.uuid == subquery.c.uuid) &
                        (GPUCardConfig.uuid == subquery.c.uuid) &
                        (GPUCardConfig.timestamp == subquery.c.max_timestamp)
                    ).order_by(None)

            async with self.make_async_session() as session:
                gpus = (await session.execute(query)).all()
                if gpus:
                    # result:
                    #   list of tuple - (GPUCard, GPUCardConfig)
                    #   merge into a single dictionary
                    gpus = [dict(x[0]) | dict(x[1]) for x in gpus]

                return gpus
        except Exception as e:
            logger.warning(e)
            raise

        return {}

    async def get_last_probe_timestamp(self, cluster: str) -> Awaitable[dict[str, dt.datetime]]:
        """
        Get the timestamp of the lastest sample for all nodes in this cluster
        """
        query = select(
                     ProcessStatus.node,
                     func.max(ProcessStatus.timestamp).label('max_timestamp')
                   ).filter(
                     ProcessStatus.cluster == cluster
                   ).group_by(
                     ProcessStatus.node
                   )

        async with self.make_async_session() as session:
            timestamps = (await session.execute(query)).all()
            if timestamps:
                # result:
                #   list of tuple - (node, max_timestamp)
                #   merge into a single dictionary
                return {x[0]: x[1] for x in timestamps}

        return {}


    async def get_nodes_gpu_process_status_timeseries(
            self,
            cluster: str,
            nodes: list[str],
            start_time_in_s: int,
            end_time_in_s: int,
            resolution_in_s: int
            ):
        """
        return:
            nodename:
                gpu_uuid:
                    job_id+_+epoch:
                        pid: Array[GPUCardProcessStatus]

        """

        node_configs = await self.get_last_node_configs(
                           cluster=cluster,
                           node=nodes,
                           start_time_in_s=start_time_in_s,
                           end_time_in_s=end_time_in_s,
                        )

        if not node_configs:
            raise RuntimeError(f"gpu_process_status: {nodes=} on {cluster=} are not available")

        nodes_gpu_process_status = {}
        for node_config in node_configs:
            cards = [x[0] for x in node_config.cards]

            # Identifiable processes in the time window
            subquery = select(
                        GPUCardProcessStatus.job,
                        GPUCardProcessStatus.epoch,
                        GPUCardProcessStatus.pid,
                        GPUCardProcessStatus.uuid
                    ).where(
                        GPUCardProcessStatus.uuid.in_(cards)
                    )

            if start_time_in_s:
                subquery = subquery.filter(
                    GPUCardProcessStatus.timestamp >= dt.datetime.fromtimestamp(start_time_in_s, dt.timezone.utc)
                )

            if end_time_in_s:
                subquery = subquery.filter(
                    GPUCardProcessStatus.timestamp <= dt.datetime.fromtimestamp(end_time_in_s, dt.timezone.utc)
                )
            subquery = subquery.distinct()

            async with self.make_async_session() as session:
                process_ids = (await session.execute(subquery)).all()

            processes = {}
            for process_id in process_ids:
                job, epoch, pid, uuid = process_id
                timeseries_data = await self.get_gpu_process_status_timeseries(
                        uuid=uuid,
                        job=job,
                        epoch=epoch,
                        start_time_in_s=start_time_in_s,
                        end_time_in_s=end_time_in_s,
                        resolution_in_s=resolution_in_s
                )

                if uuid not in processes:
                    processes[uuid] = []

                processes[uuid].append(
                    {
                      "job": job,
                      "epoch": epoch,
                      "data": timeseries_data
                    }
                )

            nodes_gpu_process_status[node_config.node] = processes

        return nodes_gpu_process_status

    async def get_gpu_process_status_timeseries(self,
            uuid: str,
            job: int,
            epoch: int,
            start_time_in_s: int,
            end_time_in_s: int,
            resolution_in_s: int) -> Awaitable[list[dict[str, any]]]:

            query = select(
                        GPUCardProcessStatus.gpu_util,
                        GPUCardProcessStatus.gpu_memory,
                        GPUCardProcessStatus.gpu_memory_util,
                        GPUCardProcessStatus.timestamp
                    ).where(
                        (GPUCardProcessStatus.job == job) &
                        (GPUCardProcessStatus.epoch == epoch) &
                        (GPUCardProcessStatus.uuid == uuid)
                    ).filter(
                        GPUCardProcessStatus.timestamp >= dt.datetime.fromtimestamp(start_time_in_s, dt.timezone.utc)
                    ).filter(
                        GPUCardProcessStatus.timestamp <= dt.datetime.fromtimestamp(end_time_in_s, dt.timezone.utc)
                    ).order_by(
                        GPUCardProcessStatus.timestamp.asc()
                    )

            async with self.make_async_session() as session:
                timeseries = (await session.execute(query)).all()
                return [
                        {
                            'gpu_util': x[0],
                            'gpu_memory': x[1],
                            'gpu_memory_util': x[2],
                            'timestamp': x[3]
                        } for x in timeseries
                ]

    async def get_node_gpu_process_util(self,
            cluster: str,
            node: str
        ):
        """
        Get the latest gpu utilization for the given node
        """

        node_config = await self.get_last_node_configs(cluster=cluster, node=node)
        if node_config:
            node_config = node_config[0]
            cards = [x[0] for x in node_config.cards]
        else:
            raise RuntimeError(f"Failed to retrieve node config for {cluster=} {node=}")

        return await self.get_gpu_process_util(uuids=cards)


    async def get_gpu_process_util(self,
            uuids: str | list[str]
        ):
        """
        Get the latest gpu utilization for the given uuids
        """

        try:
            start_time_in_s = utcnow().timestamp() - 60
            query = select(
                         GPUCardProcessStatus.uuid,
                         func.max(GPUCardProcessStatus.timestamp).label('max_timestamp')
                       ).filter(
                         (GPUCardProcessStatus.uuid.in_(uuids)) &
                         (GPUCardProcessStatus.timestamp >= dt.datetime.fromtimestamp(start_time_in_s, dt.timezone.utc))
                       ).group_by(
                         GPUCardProcessStatus.uuid
                       )

            async with self.make_async_session() as session:
                last_timestamps = (await session.execute(query)).all()

            data = {}
            for last_timestamp in last_timestamps:
                uuid, timestamp = last_timestamp

                query = select(
                            GPUCardProcessStatus.uuid,
                            GPUCardProcessStatus.gpu_memory,
                            func.sum(GPUCardProcessStatus.gpu_util),
                            func.sum(GPUCardProcessStatus.gpu_memory_util),
                        ).where(
                            (GPUCardProcessStatus.uuid == uuid) &
                            (GPUCardProcessStatus.timestamp == timestamp)
                        ).group_by(
                            GPUCardProcessStatus.uuid,
                            GPUCardProcessStatus.gpu_memory
                        )

                async with self.make_async_session() as session:
                    uuid_result = (await session.execute(query)).all()
                    if uuid_result:
                        uuid, gpu_memory, gpu_util, gpu_memory_util = uuid_result[0]
                        data[uuid] = {
                                     'gpu_memory': gpu_memory,
                                     'gpu_util': gpu_util,
                                     'gpu_memory_util': gpu_memory_util
                                 }
            return data
        except Exception as e:
            logger.warning(e)
            raise

    async def get_nodes_process_status_timeseries(
            self,
            cluster: str,
            nodes: list[str],
            start_time_in_s: int,
            end_time_in_s: int,
            resolution_in_s: int
            ):
        """
        return:
            nodename:
                Array[{job: int, epoch: int, data: Dict}]

        """
        node_configs = await self.get_last_node_configs(
                           cluster=cluster,
                           node=nodes,
                           start_time_in_s=start_time_in_s,
                           end_time_in_s=end_time_in_s,
                        )

        if not node_configs:
            raise RuntimeError(f"gpu_process_status: {nodes=} on {cluster=} are not available")

        nodes_process_status = {}
        for node_config in node_configs:
            # Identifiable processes in the time window
            subquery = select(
                        ProcessStatus.job,
                        ProcessStatus.epoch,
                        ProcessStatus.pid,
                    )

            if start_time_in_s:
                subquery = subquery.filter(
                    ProcessStatus.timestamp >= dt.datetime.fromtimestamp(start_time_in_s, dt.timezone.utc)
                )

            if end_time_in_s:
                subquery = subquery.filter(
                    ProcessStatus.timestamp <= dt.datetime.fromtimestamp(end_time_in_s, dt.timezone.utc)
                )
            subquery = subquery.distinct()

            async with self.make_async_session() as session:
                process_ids = (await session.execute(subquery)).all()

            jobs = {}
            for process_id in process_ids:
                job, epoch, pid = process_id
                timeseries_data = await self.get_process_status_timeseries(
                        job=job,
                        epoch=epoch,
                        pid=pid,
                        start_time_in_s=start_time_in_s,
                        end_time_in_s=end_time_in_s,
                        resolution_in_s=resolution_in_s
                )

                job_id = f"{job}_{epoch}"
                if job_id not in jobs:
                    jobs[job_id] = {
                      "job": job,
                      "epoch": epoch,
                      "processes": []
                    }

                jobs[job_id]["processes"].append(
                    {
                      "pid": pid,
                      "data": timeseries_data
                    }
                )

            nodes_process_status[node_config.node] = [y for x,y in jobs.items()]

        return nodes_process_status

    async def get_process_status_timeseries(self,
            job: int,
            epoch: int,
            pid: str,
            start_time_in_s: int,
            end_time_in_s: int,
            resolution_in_s: int) -> Awaitable[list[dict[str, any]]]:

            query = select(
                        ProcessStatus.resident,
                        ProcessStatus.virtual,
                        ProcessStatus.cpu_avg,
                        ProcessStatus.cpu_util,
                        ProcessStatus.cpu_time,
                        ProcessStatus.timestamp
                    ).where(
                        (ProcessStatus.job == job) &
                        (ProcessStatus.epoch == epoch) &
                        (ProcessStatus.pid == pid)
                    ).filter(
                        ProcessStatus.timestamp >= dt.datetime.fromtimestamp(start_time_in_s, dt.timezone.utc)
                    ).filter(
                        ProcessStatus.timestamp <= dt.datetime.fromtimestamp(end_time_in_s, dt.timezone.utc)
                    ).order_by(
                        ProcessStatus.timestamp.asc()
                    )

            async with self.make_async_session() as session:
                timeseries = (await session.execute(query)).all()
                return [
                        {
                            'resident': x[0],
                            'virtual': x[1],
                            'cpu_avg': x[2],
                            'cpu_util': x[3],
                            'cpu_time': x[4],
                            'timestamp': x[5]
                        } for x in timeseries
                ]

