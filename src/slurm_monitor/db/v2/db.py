import asyncio
from tqdm import tqdm
import os
from collections.abc import Awaitable
import datetime as dt
from pydantic import BaseModel
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager, asynccontextmanager
import sqlalchemy
from sqlalchemy.engine.url import URL, make_url
from sqlalchemy import (
        Integer,
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
    Cluster,
    EpochFn,
    GPUCard,
    GPUCardConfig,
    GPUCardStatus,
    GPUCardProcessStatus,
    Node,
    NodeConfig,
    ProcessStatus,
    SlurmJobStatus,
    SlurmJobAccStatus,
    SoftwareVersion,
    TableMetadata,
    TableBase,
    time_bucket
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
       return await self.fetch_first_async(db_cls=db_cls, where=where, order_by=db_cls.timestamp.desc())


class ClusterDB(Database):
    Cluster = Cluster
    Node = Node
    NodeConfig = NodeConfig
    GPUCard = GPUCard
    GPUCardConfig= GPUCardConfig
    GPUCardStatus = GPUCardStatus
    GPUCardProcessStatus = GPUCardProcessStatus

    ProcessStatus = ProcessStatus
    SoftwareVersion = SoftwareVersion
    SlurmJobStatus = SlurmJobStatus
    SlurmJobAccStatus = SlurmJobAccStatus

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
        query = select(Node.node).where(
                    Node.cluster == cluster
                ).distinct()

        async with self.make_async_session() as session:
            nodes = (await session.execute(query)).all()
            if nodes:
                nodes = [x[0] for x in nodes]
            return nodes

    async def get_gpu_nodes(self, cluster: str) -> list[str]:
        # SELECT node FROM (SELECT node, max(timestamp) as max, cards FROM node_config GROUP BY node, cards) WHERE cards != '{}';
        subquery = select(NodeConfig.node, NodeConfig.cluster, func.max(NodeConfig.timestamp), NodeConfig.cards).where(
                        NodeConfig.cluster == cluster
                    ).group_by(NodeConfig.node, NodeConfig.cluster, NodeConfig.cards).subquery()

        query = select(subquery.c.node).select_from(subquery).where(
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

    async def get_active_node_configs(self,
            cluster: str,
            node: str | list[str] | None = None,
            time_in_s: int | None = None) -> NodeConfig:
        """
        Retrieve the node configuration that is active at the given point in time
        if not time is given, the current time (as of 'now') is used
        """

        nodelist = node
        if node is None:
            nodelist = await self.get_nodes(cluster=cluster)
        elif type(node) == str:
            node = [node]

        try:
            where = (NodeConfig.cluster == cluster) & NodeConfig.node.in_(node)
            if time_in_s:
                where &= NodeConfig.timestamp <= dt.datetime.fromtimestamp(time_in_s, dt.timezone.utc)
            else:
                where &= NodeConfig.timestamp <= utcnow()

            # Find the lastest configuration in a particular timewindow
            subquery = select(
                         NodeConfig.cluster,
                         NodeConfig.node,
                         func.max(NodeConfig.timestamp).label('max_timestamp')
                       ).where(
                         where
                       ).group_by(
                         NodeConfig.cluster,
                         NodeConfig.node,
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
                node_config = [x[0] for x in (await session.execute(query)).all()]
                if node_config or time_in_s is None:
                    return node_config

            # if here then we fallback to the closest known node configuration to the given
            # timepoint
            # Find the latest configuration in a particular time window
            subquery = select(
                         NodeConfig.cluster,
                         NodeConfig.node,
                         func.min(NodeConfig.timestamp).label('min_timestamp')
                       ).where(
                            (NodeConfig.cluster == cluster) &
                            NodeConfig.node.in_(node) &
                            (NodeConfig.timestamp >= dt.datetime.fromtimestamp(time_in_s, dt.timezone.utc))
                       ).group_by(
                         NodeConfig.cluster,
                         NodeConfig.node,
                       ).subquery()

            query = select(
                        NodeConfig
                    ).join(
                        subquery,
                        (NodeConfig.cluster == subquery.c.cluster) &
                        (NodeConfig.node == subquery.c.node) &
                        (NodeConfig.timestamp == subquery.c.min_timestamp)
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

            node_config = await self.get_active_node_configs(
                                    cluster=cluster, node=node,
                                    time_in_s=start_time_in_s,
                            )

            if node_config:
                node_config = node_config[0]
                cards = [x[0] for x in node_config.cards]
            else:
                raise RuntimeError(f"Failed to retrieve node config for {cluster=} {node=}")


            where = GPUCardConfig.uuid.in_(cards)
            if start_time_in_s:
                where &= GPUCardConfig.timestamp >= dt.datetime.fromtimestamp(start_time_in_s, dt.timezone.utc)

            if end_time_in_s:
                where &= GPUCardConfig.timestamp <= dt.datetime.fromtimestamp(end_time_in_s, dt.timezone.utc)

            # Subquery to get the latest GPUCardConfig within a timewindow
            subquery = select(
                           GPUCardConfig.uuid,
                           func.max(GPUCardConfig.timestamp).label('max_timestamp')
                       ).where(
                           where
                       ).group_by(
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
                   ).where(
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
            job_id: int | None,
            epoch: int,
            start_time_in_s: int,
            end_time_in_s: int,
            resolution_in_s: int
            ):
        """
        Get the GPUCardProcessStatus timeseries for a set of nodes in a cluster.

        return:
            nodename:
                gpu_uuid: Array[{job: int, epoch: int, data: Array[GPUCardProcessStatus]]
        """
        node_configs = await self.get_active_node_configs(
                           cluster=cluster,
                           node=nodes,
                           time_in_s=start_time_in_s,
                        )

        if not node_configs:
            raise RuntimeError(f"gpu_process_status: {nodes=} on {cluster=} are not available")

        nodes_gpu_process_status = {}
        for node_config in node_configs:
            cards = [x[0] for x in node_config.cards]

            where = GPUCardProcessStatus.uuid.in_(cards)
            if job_id is not None:
                where &= (GPUCardProcessStatus.job == job_id) & (GPUCardProcessStatus.epoch == epoch)

            if start_time_in_s:
                where &= GPUCardProcessStatus.timestamp >= dt.datetime.fromtimestamp(start_time_in_s, dt.timezone.utc)

            if end_time_in_s:
                where &= GPUCardProcessStatus.timestamp <= dt.datetime.fromtimestamp(end_time_in_s, dt.timezone.utc)

            # Identifiable processes in the time window
            subquery = select(
                        GPUCardProcessStatus.job,
                        GPUCardProcessStatus.epoch,
                        GPUCardProcessStatus.pid,
                        GPUCardProcessStatus.uuid
                    ).where(
                        where
                    ).distinct()

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

    async def get_jobs_system_process_status_timeseries(
            self,
            cluster: str,
            job_id: int | None,
            epoch: int,
            start_time_in_s: int,
            end_time_in_s: int,
            resolution_in_s: int,
            nodes: list[str] | None = None
            ):

        gpu_status = await self.get_jobs_gpu_process_status_timeseries(
                cluster=cluster,
                job_id=job_id,
                epoch=epoch,
                start_time_in_s=start_time_in_s,
                end_time_in_s=end_time_in_s,
                resolution_in_s=resolution_in_s,
                nodes=nodes
            )

        active_jobs = await self.get_active_jobs(
                cluster=cluster,
                start_time_in_s=start_time_in_s,
                end_time_in_s=end_time_in_s
            )

        all_jobs = []
        for job in active_jobs:
            jid, job_epoch, job_nodes = job

            if job_id is not None and epoch is not None:
                if jid != job_id or job_epoch != epoch:
                    continue

            if nodes is None:
                nodes = job_nodes

            nodes_data = {}
            for gpu_job in gpu_status:
                if jid == gpu_job['job'] and epoch == gpu_job["epoch"]:
                    nodes_data = gpu_job['nodes']
                    break

            for node in nodes:
                cpu_status_timeseries = await self.get_cpu_status_timeseries(
                        cluster=cluster,
                        job_id=job_id,
                        node=node,
                        epoch=job_epoch,
                        start_time_in_s=start_time_in_s,
                        end_time_in_s=end_time_in_s,
                        resolution_in_s=resolution_in_s
                )
                if cpu_status_timeseries:
                    if node not in nodes_data:
                        nodes_data[node] = {}

                    nodes_data[node]['cpu_memory'] = cpu_status_timeseries

            all_jobs.append({ 'job': jid, 'epoch': epoch, 'nodes': nodes_data})

        return all_jobs

    async def get_active_jobs(self, cluster: str,
                start_time_in_s: int,
                end_time_in_s: int
                ):
        """
            Get active job
            return
                [ (job, epoch, nodes) ]
        """
        query = select(
                    ProcessStatus.job,
                    ProcessStatus.epoch,
                    func.array_agg(ProcessStatus.node.distinct())
                ).where(
                    (ProcessStatus.cluster == cluster) &
                    (ProcessStatus.timestamp >= dt.datetime.fromtimestamp(start_time_in_s, dt.timezone.utc)) &
                    (ProcessStatus.timestamp <= dt.datetime.fromtimestamp(end_time_in_s, dt.timezone.utc))
                ).group_by(
                    ProcessStatus.job,
                    ProcessStatus.epoch
                ).distinct()

        async with self.make_async_session() as session:
            return (await session.execute(query)).all()


    async def get_jobs_gpu_process_status_timeseries(
            self,
            cluster: str,
            job_id: int | None,
            epoch: int,
            start_time_in_s: int,
            end_time_in_s: int,
            resolution_in_s: int,
            nodes: list[str] | None = None
            ):
        """
        Get the GPUCardProcessStatus timeseries for jobs in a cluster.

        return:
            Array[{job: int, epoch: int, nodes: { node: { uuid:  Array[GPUCardProcessStatus]}}}]
        """

        where = GPUCardProcessStatus.cluster == cluster
        if nodes:
            where &= GPUCardProcessStatus.node.in_(nodes)

        if job_id is not None:
            where &= (GPUCardProcessStatus.job == job_id) & (GPUCardProcessStatus.epoch == epoch)

        if start_time_in_s:
            where &= GPUCardProcessStatus.timestamp >= dt.datetime.fromtimestamp(start_time_in_s, dt.timezone.utc)

        if end_time_in_s:
            where &= GPUCardProcessStatus.timestamp <= dt.datetime.fromtimestamp(end_time_in_s, dt.timezone.utc)

        query = select(
                    GPUCardProcessStatus.job,
                    GPUCardProcessStatus.epoch,
                    func.array_agg(GPUCardProcessStatus.node.distinct()),
                ).where(
                    where
                ).group_by(
                    GPUCardProcessStatus.job,
                    GPUCardProcessStatus.epoch
                ).distinct()

        async with self.make_async_session() as session:
            jobs = (await session.execute(query)).all()


        job_gpu_process_status = []
        for job in jobs:
            job_id, job_epoch, job_nodes = job
            nodes = {}
            for node in job_nodes:
                # Identifiable processes in the time window
                query = select(
                            GPUCardProcessStatus.uuid
                        ).where(
                            (GPUCardProcessStatus.job == job_id) &
                            (GPUCardProcessStatus.epoch == epoch) &
                            (GPUCardProcessStatus.node == node)
                        ).distinct()

                async with self.make_async_session() as session:
                    uuids = [x[0] for x in (await session.execute(query)).all()]


                card_data = {}
                for uuid in uuids:
                    card_data[uuid] = await self.get_gpu_process_status_timeseries(
                        uuid=uuid,
                        job=job_id,
                        epoch=job_epoch,
                        start_time_in_s=start_time_in_s,
                        end_time_in_s=end_time_in_s,
                        resolution_in_s=resolution_in_s
                    )
                nodes[node] = { 'gpus': card_data }
            job_gpu_process_status.append({'job': job_id, 'epoch': epoch, 'nodes': nodes})
        return job_gpu_process_status

    async def get_gpu_process_status_timeseries(self,
            uuid: str,
            job: int,
            epoch: int,
            start_time_in_s: int,
            end_time_in_s: int,
            resolution_in_s: int) -> Awaitable[list[dict[str, any]]]:
        """
        Get the GPUCardProcessStatus timeseries for a given GPU (by uuid) and a job
        identified by job and epoch

        return
            Array[{ gpu_util: float, gpu_memory: float, gpu_memory_util: float, pids: list[int], timestamp: dt.datetime}]

        """
        query = select(
                    func.avg(GPUCardProcessStatus.gpu_util),
                    func.avg(GPUCardProcessStatus.gpu_memory),
                    func.avg(GPUCardProcessStatus.gpu_memory_util),
                    func.array_agg(GPUCardProcessStatus.pid.distinct()),
                    time_bucket(resolution_in_s, GPUCardProcessStatus.timestamp).label('time_bucket')
                ).where(
                    (GPUCardProcessStatus.job == job) &
                    (GPUCardProcessStatus.epoch == epoch) &
                    (GPUCardProcessStatus.uuid == uuid) &
                    (GPUCardProcessStatus.timestamp >= dt.datetime.fromtimestamp(start_time_in_s, dt.timezone.utc)) &
                    (GPUCardProcessStatus.timestamp <= dt.datetime.fromtimestamp(end_time_in_s, dt.timezone.utc))
                ).group_by(
                    'time_bucket',
                ).order_by(
                    'time_bucket'
                )

        async with self.make_async_session() as session:
            timeseries = (await session.execute(query)).all()
            return [
                    {
                        'gpu_util': x[0],
                        'gpu_memory': x[1],
                        'gpu_memory_util': x[2],
                        'pids': x[3],
                        'timestamp': x[4]
                    } for x in timeseries
            ]

    async def get_node_gpu_process_util(self,
            cluster: str,
            node: str,
            reference_time_in_s: float | None = None,
            window_in_s: int | None = None,
        ):
        """
        Get the latest gpu utilization for the given node
        """

        node_config = await self.get_active_node_configs(
                    cluster=cluster,
                    node=node,
                    time_in_s=reference_time_in_s - window_in_s
                )
        if node_config:
            node_config = node_config[0]
            cards = [x[0] for x in node_config.cards]
        else:
            raise RuntimeError(f"Failed to retrieve node config for {cluster=} {node=}")

        return await self.get_gpu_process_util(uuids=cards,
                reference_time_in_s=reference_time_in_s,
                window_in_s=window_in_s
                )


    async def get_gpu_process_util(self,
            uuids: str | list[str],
            reference_time_in_s: float | None = None,
            window_in_s: int | None = None,
        ):
        """
        Get the latest gpu utilization for the given uuids
        """
        if reference_time_in_s is None:
            reference_time_in_s = utcnow().timestamp()

        if window_in_s is None:
            window_in_s = 60

        try:
            start_time_in_s = reference_time_in_s - window_in_s
            query = select(
                         GPUCardProcessStatus.uuid,
                         func.max(GPUCardProcessStatus.timestamp).label('max_timestamp')
                       ).where(
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
        node_configs = await self.get_active_node_configs(
                           cluster=cluster,
                           node=nodes,
                           time_in_s=start_time_in_s,
                        )

        if not node_configs:
            raise RuntimeError(f"gpu_process_status: {nodes=} on {cluster=} are not available")

        nodes_process_status = {}
        for node_config in node_configs:
            where = (ProcessStatus.cluster == cluster) & (ProcessStatus.node == node_config.node)
            if start_time_in_s:
                where &= ProcessStatus.timestamp >= dt.datetime.fromtimestamp(start_time_in_s, dt.timezone.utc)

            if end_time_in_s:
                where &= ProcessStatus.timestamp <= dt.datetime.fromtimestamp(end_time_in_s, dt.timezone.utc)

            # Identifiable processes in the time window
            subquery = select(
                        ProcessStatus.job,
                        ProcessStatus.epoch,
                        ProcessStatus.pid,
                    ).where(
                        where
                    ).distinct()

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
                        memory=node_config.memory,
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
            memory: int,
            start_time_in_s: int,
            end_time_in_s: int,
            resolution_in_s: int) -> Awaitable[list[dict[str, any]]]:

            query = select(
                        func.avg(ProcessStatus.resident),
                        func.avg(ProcessStatus.virtual),
                        (func.avg(ProcessStatus.resident) / memory).label('memory_util'),
                        func.avg(ProcessStatus.cpu_avg),
                        func.avg(ProcessStatus.cpu_util),
                        func.avg(ProcessStatus.cpu_time),
                        time_bucket(resolution_in_s, ProcessStatus.timestamp).label("bucket"),
                    ).where(
                        (ProcessStatus.job == job) &
                        (ProcessStatus.epoch == epoch) &
                        (ProcessStatus.pid == pid)
                    ).filter(
                        ProcessStatus.timestamp >= dt.datetime.fromtimestamp(start_time_in_s, dt.timezone.utc)
                    ).filter(
                        ProcessStatus.timestamp <= dt.datetime.fromtimestamp(end_time_in_s, dt.timezone.utc)
                    ).group_by(
                       'bucket',
                    ).order_by(
                       'bucket'
                    )

            async with self.make_async_session() as session:
                timeseries = (await session.execute(query)).all()
                return [
                        {
                            'memory_resident': x[0],
                            'memory_virtual': x[1],
                            'memory_util': round(x[2]*100,2),
                            'cpu_avg': x[3],
                            'cpu_util': x[4],
                            'cpu_time': x[5],
                            'timestamp': x[6]
                        } for x in timeseries
                ]

    async def get_cpu_status_timeseries(self,
            cluster: str,
            node: str,
            job_id: int | None,
            start_time_in_s: int,
            end_time_in_s: int,
            resolution_in_s: int,
            epoch: int = 0,
            ) -> Awaitable[list[dict[str, any]]]:
            """
            Get CPU and memory sampling aggregated either for all jobs, or per job id
            """

            query = select(
                        NodeConfig.memory,
                        NodeConfig.timestamp
                   ).where(
                        (NodeConfig.cluster == cluster),
                        (NodeConfig.node == node),
                   )

            async with self.make_async_session() as session:
                node_config = (await session.execute(query)).all()
                if node_config:
                    memory = None
                    for n in node_config:
                        if memory is None:
                            memory, timestamp = n
                        else:
                            if memory != n[0]:
                                raise RuntimeError("cpu_status: memory changed - might lead to inconsistent reporting")
                else:
                    raise RuntimeError(f"cpu_status: missing node config for {node=}")

            where = (ProcessStatus.cluster == cluster) & (ProcessStatus.node == node)
            if job_id:
                where &= (ProcessStatus.job == job_id) & (ProcessStatus.epoch == epoch)

            subquery = select(
                        func.sum(ProcessStatus.resident).label('resident'),
                        func.sum(ProcessStatus.virtual).label('virtual'),
                        func.sum(ProcessStatus.cpu_avg).label('cpu_avg'),
                        func.sum(ProcessStatus.cpu_util).label('cpu_util'),
                        func.sum(ProcessStatus.cpu_time).label('cpu_time'),
                        func.array_agg(func.cast(ProcessStatus.pid, Integer).distinct()).label('pids'),
                        ProcessStatus.timestamp
                    ).where(
                        where &
                        (ProcessStatus.timestamp >= dt.datetime.fromtimestamp(start_time_in_s, dt.timezone.utc)) &
                        (ProcessStatus.timestamp <= dt.datetime.fromtimestamp(end_time_in_s, dt.timezone.utc))
                    ).group_by(
                       ProcessStatus.timestamp,
                    ).order_by(
                       ProcessStatus.timestamp.asc()
                    )

            subquery = subquery.subquery()

            query = select(
                        func.avg(subquery.c.resident),
                        func.avg(subquery.c.virtual),
                        (func.avg(subquery.c.resident)/memory).label("memory_util"),
                        func.avg(subquery.c.cpu_avg),
                        func.avg(subquery.c.cpu_util),
                        func.avg(subquery.c.cpu_time),
                        func.avg(func.distinct(func.array_length(subquery.c.pids, 1))),
                        time_bucket(resolution_in_s, subquery.c.timestamp).label("bucket"),
                    ).select_from(
                        subquery
                    ).group_by(
                       'bucket',
                    ).order_by(
                       'bucket'
                    )

            async with self.make_async_session() as session:
                timeseries = (await session.execute(query)).all()
                return [
                        {
                            'memory_resident': x[0],
                            'memory_virtual': x[1],
                            'memory_util': round(100*x[2],2),
                            'cpu_avg': x[3],
                            'cpu_util': x[4],
                            'cpu_time': x[5],
                            'pids': x[6],
                            'timestamp': x[7]
                        } for x in timeseries
                ]

    async def get_gpu_status_timeseries(
            self,
            cluster: str,
            node: str,
            start_time_in_s: int,
            end_time_in_s: int,
            resolution_in_s: int
            ):
        """
        Get the latest GPU status data for a given timeframe.
        For all cards on a node:
        return:
            [ { uuid: string, local_index: string, data: Array[GPUCardProcessStatus]} ]
        """
        query = select(
                NodeConfig.cluster,
                NodeConfig.node,
                NodeConfig.cards,
                func.max(NodeConfig.timestamp)
               ).where(
                    (NodeConfig.cluster == cluster),
                    (NodeConfig.node == node)
               ).group_by(
                   NodeConfig.cluster,
                   NodeConfig.node,
                   NodeConfig.cards
               )

        async with self.make_async_session() as session:
            node_configs = (await session.execute(query)).all()

        gpu_status = []
        for node_config in node_configs:
            cards = [x[0] for x in node_config[2]]

            for card_uuid in cards:

                where = GPUCardStatus.uuid == card_uuid
                if start_time_in_s:
                    where &= (GPUCardProcessStatus.timestamp >= dt.datetime.fromtimestamp(start_time_in_s, dt.timezone.utc))

                if end_time_in_s:
                    where &= (GPUCardProcessStatus.timestamp <= dt.datetime.fromtimestamp(end_time_in_s, dt.timezone.utc))

                query = select(
                        func.count(GPUCardStatus.failing).label("failure_count"),
                        func.avg(GPUCardStatus.memory),
                        func.avg(GPUCardStatus.memory_util),
                        func.avg(GPUCardStatus.ce_util),
                        func.avg(GPUCardStatus.temperature),
                        func.avg(GPUCardStatus.power),
                        func.avg(GPUCardStatus.power_limit),
                        func.avg(GPUCardStatus.memory_clock),
                        func.min(GPUCardStatus.index),
                        time_bucket(resolution_in_s, GPUCardProcessStatus.timestamp).label('bucket')
                    ).where(
                        where
                    ).group_by(
                        'bucket'
                    ).order_by(
                        'bucket'
                    )

                query = query.group_by(
                            'bucket'
                        ).order_by(
                            'bucket'
                        )

                async with self.make_async_session() as session:
                    node_samples = (await session.execute(query)).all()
                    if node_samples:
                        local_index = node_samples[0][8]
                        timeseries_data = [{
                                'failure_count': x[0],
                                'memory': round(x[1],2),
                                'memory_util': round(x[2],2),
                                'ce_util': round(x[3],2),
                                'temperature': round(x[4],2),
                                'power': round(x[5],2),
                                'power_limit': round(x[6],2),
                                'memory_clock': round(x[7],2),
                                'timestamp': x[9]
                            } for x in node_samples]

                        gpu_status.append({'uuid': card_uuid, 'local_index': local_index, 'data': timeseries_data })

        return gpu_status


    async def get_jobs(
            self,
            cluster: str,
            start_time_in_s: int | None = None,
            end_time_in_s: int | None = None,
        ):
        return await self.get_slurm_jobs(cluster,
                    start_time_in_s=start_time_in_s,
                    end_time_in_s=end_time_in_s,
                )

    async def get_slurm_job(
            self,
            cluster: str,
            job_id: int,
            start_time_in_s: int | None = None,
            end_time_in_s: int | None = None,
            states: list[str] | None = None,
        ) -> dict[str, any] | None:
        """
        Get the SLURM job status for all a specific job in a cluster
        """
        where = (SlurmJobStatus.cluster == cluster) & (SlurmJobStatus.job_id == job_id) & (SlurmJobStatus.job_step == '') # get the main job
        if start_time_in_s:
            where &= (SlurmJobStatus.timestamp >= dt.datetime.fromtimestamp(start_time_in_s, dt.timezone.utc))
        if end_time_in_s:
            where &= (SlurmJobStatus.timestamp <= dt.datetime.fromtimestamp(end_time_in_s, dt.timezone.utc))

        query = select(
                    SlurmJobStatus
                ).where(
                    where
                ).order_by(
                    SlurmJobStatus.timestamp.desc()
                ).limit(1)

        gpus_query = select(
                    GPUCardProcessStatus.uuid.distinct()
                ).where(
                    (GPUCardProcessStatus.cluster == cluster) &
                    (GPUCardProcessStatus.job == job_id) &
                    (GPUCardProcessStatus.epoch == 0)
                )

        async with self.make_async_session() as session:
            data = (await session.execute(query)).all()
            gpus = [x[0] for x in (await session.execute(gpus_query)).all()]

            if data:
                slurm_data =  dict(data[0][0])
                if gpus:
                    slurm_data['used_gpu_uuids'] = gpus
                return slurm_data
            else:
                return None

    async def query_jobs(self,
            cluster: str,
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
            where &= SlurmJobStatus.user_name == user

        if user_id:
            where &= SlurmJobStatus.user_id == user_id

        if job_id:
            where &= SlurmJobStatus.job_id == job_id

        if start_before_in_s is not None:
            reference_time = dt.datetime.fromtimestamp(start_before_in_s, dt.timezone.utc).replace(tzinfo=None)
            where &= SlurmJobStatus.start_time <= reference_time

        if start_after_in_s is not None:
            reference_time = dt.datetime.fromtimestamp(start_after_in_s, dt.timezone.utc).replace(tzinfo=None)
            where &= SlurmJobStatus.start_time >= reference_time

        if end_before_in_s is not None:
            reference_time = dt.datetime.fromtimestamp(end_before_in_s, dt.timezone.utc).replace(tzinfo=None)
            where &= SlurmJobStatus.end_time <= reference_time

        if end_after_in_s is not None:
            reference_time = dt.datetime.fromtimestamp(end_after_in_s, dt.timezone.utc).replace(tzinfo=None)
            where &= SlurmJobStatus.end_time >= reference_time

        if submit_before_in_s is not None:
            reference_time = dt.datetime.fromtimestamp(submit_before_in_s, dt.timezone.utc).replace(tzinfo=None)
            where &= SlurmJobStatus.submit_time <= reference_time

        if submit_after_in_s is not None:
            reference_time = dt.datetime.fromtimestamp(submit_after_in_s, dt.timezone.utc).replace(tzinfo=None)
            where &= SlurmJobStatus.submit_time >= reference_time

        if min_duration_in_s is not None:
            where &= (EpochFn(SlurmJobStatus.end_time) - EpochFn(SlurmJobStatus.start_time)) >= min_duration_in_s

        if max_duration_in_s is not None:
            where &= (EpochFn(SlurmJobStatus.end_time) - EpochFn(SlurmJobStatus.start_time)) <= max_duration_in_s

        # limit search to completed jobs
        where &= (SlurmJobStatus.job_state == 'COMPLETED') & (SlurmJobStatus.user_name != '')
        return await self.fetch_all_async(SlurmJobStatus, where=where, limit=limit)


    async def get_slurm_jobs(
            self,
            cluster: str,
            start_time_in_s: int | None = None,
            end_time_in_s: int | None = None,
        ):
        """
        Get the SLURM job status for all jobs in a cluster
        """
        if end_time_in_s is None:
            end_time_in_s = utcnow().timestamp()

        if start_time_in_s is None:
            start_time_in_s = end_time_in_s - 300

        subquery = select(
                SlurmJobStatus.job_id,
                SlurmJobStatus.job_step,
                func.max(SlurmJobStatus.timestamp).label("timestamp")
            ).where(
                (SlurmJobStatus.cluster == cluster) &
                (SlurmJobStatus.timestamp >= dt.datetime.fromtimestamp(start_time_in_s, dt.timezone.utc)) &
                (SlurmJobStatus.timestamp <= dt.datetime.fromtimestamp(end_time_in_s, dt.timezone.utc))
            ).group_by(
                SlurmJobStatus.job_id,
                SlurmJobStatus.job_step
            ).subquery()

        query = select(
                    SlurmJobStatus,
                    SlurmJobAccStatus
                ).join(subquery,
                    (subquery.c.job_id == SlurmJobStatus.job_id) &
                    (subquery.c.job_step == SlurmJobStatus.job_step) &
                    (subquery.c.timestamp == SlurmJobStatus.timestamp)
                ).join(SlurmJobAccStatus,
                    (SlurmJobStatus.cluster == cluster) &
                    (SlurmJobStatus.job_id == SlurmJobAccStatus.job_id) &
                    (SlurmJobStatus.job_step == SlurmJobAccStatus.job_step) &
                    (SlurmJobStatus.timestamp == SlurmJobAccStatus.timestamp)
                )

        async with self.make_async_session() as session:
            data = (await session.execute(query)).all()

            samples = []
            for sample in data:
                new_sample = dict(sample[0])
                if sample[1]:
                    new_sample['sacct'] =  dict(sample[1])
                    del new_sample['sacct']['job_id']
                    del new_sample['sacct']['job_step']
                    del new_sample['sacct']['timestamp']

                samples.append(new_sample)

            return samples
