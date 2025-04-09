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
    Node,
    NodeState,
    Partition,
    SampleGpu,
    SampleProcess,
    SampleProcessGpu,
    SampleSlurmJob,
    SampleSlurmJobAcc,
    SysinfoAttributes,
    SysinfoGpuCard,
    SysinfoGpuCardConfig,
    SysinfoSoftwareVersion,
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
       return await self.fetch_first_async(db_cls=db_cls, where=where, order_by=db_cls.time.desc())


class ClusterDB(Database):
    Cluster = Cluster
    Node = Node
    NodeState = NodeState
    Partition = Partition

    SampleGpu = SampleGpu
    SampleProcess = SampleProcess
    SampleProcessGpu = SampleProcessGpu

    SampleSlurmJob = SampleSlurmJob
    SampleSlurmJobAcc = SampleSlurmJobAcc

    SysinfoAttributes = SysinfoAttributes
    SysinfoGpuCard = SysinfoGpuCard
    SysinfoGpuCardConfig = SysinfoGpuCardConfig
    SysinfoSoftwareVersion = SysinfoSoftwareVersion

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
                    SampleProcess.job,
                    SampleProcess.epoch,
                    SampleProcess.pid,
                    func.max(SampleProcess.time),
                ).group_by(
                    SampleProcess.job,
                    SampleProcess.epoch,
                    SampleProcess.pid
                )

        with self.make_writeable_session() as session:
            jobs = session.execute(query).all()

        if to_time is None:
            to_time = utcnow()

        for job in tqdm(jobs, desc="jobs"):
            job, epoch, pid, timestamp = job

            query = select(
                        SampleProcess
                    ).where(
                        ((SampleProcess.time) == timestamp) &
                        (SampleProcess.job == job) &
                        (SampleProcess.epoch == epoch) &
                        (SampleProcess.pid == pid)
                    )

            with self.make_writeable_session() as session:
                processes = session.execute(query).all()

            timeseries = []
            samples_to_add = int((to_time - timestamp).total_seconds()/ sampling_interval_in_s)
            for i in tqdm(range(0, samples_to_add), desc=f"samples for {job=} {epoch=} {pid=}"):
                timestamp += dt.timedelta(seconds=sampling_interval_in_s)
                for process in processes:
                    sample = dict(process[0])
                    sample["time"] = timestamp

                    timeseries.append( SampleProcess(**sample) )

            logger.info(f"Inserting {len(timeseries)} samples for {job=} {epoch=} {pid=}")
            self.insert(timeseries)



    def fake_timeseries_gpu_process(self,
            start_time: dt.datetime | None = None,
            to_time: dt.datetime | None = None,
            sampling_interval_in_s = 60,
        ) -> list[str]:

        # Continue GPU data
        query = select(
                    SampleProcessGpu.uuid,
                    func.max(SampleProcessGpu.time)
                ).group_by(
                    SampleProcessGpu.uuid
                )

        with self.make_writeable_session() as session:
            gpus = session.execute(query).all()

        if to_time is None:
            to_time = utcnow()

        for gpu in tqdm(gpus, desc="gpus"):
            uuid, timestamp = gpu

            query = select(
                        SampleProcessGpu
                    ).where(
                        ((SampleProcessGpu.time) == timestamp) &
                        (SampleProcessGpu.uuid == uuid)
                    )
            with self.make_writeable_session() as session:
                processes = session.execute(query).all()

            timeseries = []
            samples_to_add = int((to_time - timestamp).total_seconds()/ sampling_interval_in_s)
            for i in tqdm(range(0, samples_to_add), desc=f"samples for {uuid}"):
                timestamp += dt.timedelta(seconds=sampling_interval_in_s)
                for process in processes:
                    sample = dict(process[0])
                    sample["time"] = timestamp

                    timeseries.append( SampleProcessGpu(**sample) )

            logger.info(f"Inserting {len(timeseries)} samples for {uuid}")
            self.insert(timeseries)


    async def get_nodes(self, cluster: str, time_in_s: int | None = None) -> list[str]:
        where = Cluster.cluster == cluster
        if time_in_s:
            where &= (Cluster.time <= dt.datetime.fromtimestamp(time_in_s, dt.timezone.utc))

        query = select(Cluster.nodes).where(where)

        async with self.make_async_session() as session:
            nodes = (await session.execute(query)).all()
            if nodes:
                return nodes[0][0]
            return nodes

    async def get_gpu_nodes(self, cluster: str) -> list[str]:
        # SELECT node FROM (SELECT node, max(timestamp) as max, cards FROM node_config GROUP BY node, cards) WHERE cards != '{}';
        subquery = select(SysinfoAttributes.node, SysinfoAttributes.cluster, func.max(SysinfoAttributes.time), SysinfoAttributes.cards).where(
                        SysinfoAttributes.cluster == cluster
                    ).group_by(SysinfoAttributes.node, SysinfoAttributes.cluster, SysinfoAttributes.cards).subquery()

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


## NODES #####################################
    async def get_nodes_partitions(self,
            cluster,
            nodelist: list[str] | str | None = None,
            time_in_s: int | None = None
            ):
        if not nodelist:
            nodelist = await self.get_nodes(cluster=cluster, time_in_s=time_in_s)
        elif type(nodelist) == str:
            nodelist = [nodelist]

        where = Partition.cluster == cluster
        if time_in_s:
            where &= (Partition.time <= dt.datetime.fromtimestamp(time_in_s, dt.timezone.utc))

        query = select(
                    Partition.partition,
                    Partition.nodes
                ).where(
                    where
                )

        nodes_partitions = {}
        async with self.make_async_session() as session:
            for partition in (await session.execute(query)).all():
                for n in partition[1]:
                    value = nodes_partitions.get(n, [])
                    nodes_partitions[n] = value + [partition[0]]

        return nodes_partitions

    async def get_nodes_info(self, 
            cluster: str, 
            nodelist: list[str] | str | None = None, 
            time_in_s: int | None = None
        ):

        if not nodelist:
            nodelist = await self.get_nodes(cluster=cluster, time_in_s=time_in_s)
        elif type(nodelist) == str:
            nodelist = [nodelist]

        node_configs = await self.fetch_all_async(SysinfoAttributes,
                    (SysinfoAttributes.cluster == cluster) & (SysinfoAttributes.node.in_(nodelist)),
                    order_by=SysinfoAttributes.node
                )

        nodes_partitions = await self.get_nodes_partitions(cluster=cluster,
                nodelist=nodelist,
                time_in_s=time_in_s
        )

        nodeinfo = {}
        for node_config in node_configs:
            nodename = node_config.node
            nodeinfo[nodename] = dict(node_config)
           
            if nodename in nodes_partitions:
                nodeinfo[nodename].update({'partitions': nodes_partitions[nodename]})

            if node_config.cards:
                try:
                    nodeinfo[nodename].update({'cards': await self.get_gpu_infos(
                        cluster=cluster,
                        node=nodename,
                        time_in_s=time_in_s
                        )})
                except Exception as e:
                    logger.warn(f"Internal error: Retrieving GPU info for {nodename} failed -- {e}")
        return nodeinfo

    async def get_nodes_states(self,
            cluster: str,
            nodelist: list[str] | str | None = None,
            time_in_s: int | None = None):

        where = (NodeState.cluster == cluster)
        if nodelist:
            if type(nodelist) == str:
                nodelist = [nodelist]

            where &= NodeState.node.in_(nodelist)

        if time_in_s:
            where &= (NodeState.time <= dt.datetime.fromtimestamp(time_in_s, dt.timezone.utc))

        query = select(
                    NodeState.node,
                    NodeState.states,
                    func.max(NodeState.time),
                ).where(
                  where
                ).group_by(
                    NodeState.node,
                    NodeState.states
                )

        async with self.make_async_session() as session:
            node_states = [
                    { 
                        'node': x[0],
                        'states': x[1],
                        'time': x[2]
                    }
                    for x in (await session.execute(query)).all()]

            return node_states

    async def get_active_node_configs(self,
            cluster: str,
            node: str | list[str] | None = None,
            time_in_s: int | None = None) -> SysinfoAttributes:
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
            where = (SysinfoAttributes.cluster == cluster) & SysinfoAttributes.node.in_(node)
            if time_in_s:
                where &= SysinfoAttributes.time <= dt.datetime.fromtimestamp(time_in_s, dt.timezone.utc)
            else:
                where &= SysinfoAttributes.time <= utcnow()

            # Find the lastest configuration in a particular timewindow
            subquery = select(
                         SysinfoAttributes.cluster,
                         SysinfoAttributes.node,
                         func.max(SysinfoAttributes.time).label('max_time')
                       ).where(
                         where
                       ).group_by(
                         SysinfoAttributes.cluster,
                         SysinfoAttributes.node,
                       ).subquery()

            query = select(
                        SysinfoAttributes
                    ).join(
                        subquery,
                        (SysinfoAttributes.cluster == subquery.c.cluster) &
                        (SysinfoAttributes.node == subquery.c.node) &
                        (SysinfoAttributes.time == subquery.c.max_time)
                    ).order_by(None)


            async with self.make_async_session() as session:
                node_config = [x[0] for x in (await session.execute(query)).all()]
                if node_config or time_in_s is None:
                    return node_config

            # if here then we fallback to the closest known node configuration to the given
            # timepoint
            # Find the latest configuration in a particular time window
            subquery = select(
                         SysinfoAttributes.cluster,
                         SysinfoAttributes.node,
                         func.min(SysinfoAttributes.time).label('min_time')
                       ).where(
                            (SysinfoAttributes.cluster == cluster) &
                            SysinfoAttributes.node.in_(node) &
                            (SysinfoAttributes.time >= dt.datetime.fromtimestamp(time_in_s, dt.timezone.utc))
                       ).group_by(
                         SysinfoAttributes.cluster,
                         SysinfoAttributes.node,
                       ).subquery()

            query = select(
                        SysinfoAttributes
                    ).join(
                        subquery,
                        (SysinfoAttributes.cluster == subquery.c.cluster) &
                        (SysinfoAttributes.node == subquery.c.node) &
                        (SysinfoAttributes.time == subquery.c.min_time)
                    ).order_by(None)

            async with self.make_async_session() as session:
                return [x[0] for x in (await session.execute(query)).all()]

        except Exception as e:
            logger.warning(e)
            raise

        return []

################################################################

    async def get_active_sysinfo_gpu_card(self,
            cluster: str,
            node: str | list[str] | None = None,
            time_in_s: int | None = None) -> list[SysinfoGpuCard]:
        """
        Retrieve the GPUCard configuration that is active at the given point in time
        """
        nodelist = node
        if node is None:
            nodelist = await self.get_nodes(cluster=cluster)
        elif type(node) == str:
            node = [node]

        try:
            # Configuration needs to be active before or at that timepoint gvein
            where = (SysinfoGpuCardConfig.cluster == cluster) & SysinfoGpuCardConfig.node.in_(node)
            if time_in_s:
                where &= SysinfoGpuCardConfig.time <= dt.datetime.fromtimestamp(time_in_s, dt.timezone.utc)
            else:
                where &= SysinfoGpuCardConfig.time <= utcnow()

            # Find the lastest configuration in a particular timewindow
            subquery = select(
                         SysinfoGpuCardConfig.uuid,
                         func.max(SysinfoGpuCardConfig.time).label('max_time')
                       ).where(
                         where
                       ).group_by(
                         SysinfoGpuCardConfig.uuid
                       ).subquery()

            query = select(
                        SysinfoGpuCard,
                        SysinfoGpuCardConfig
                    ).join(
                        subquery,
                        (SysinfoGpuCardConfig.uuid == subquery.c.uuid) &
                        (SysinfoGpuCardConfig.time == subquery.c.max_time),
                    ).join(
                        SysinfoGpuCard,
                        (SysinfoGpuCard.uuid == SysinfoGpuCardConfig.uuid)
                    ).order_by(None)


            async with self.make_async_session() as session:
                gpu_cards = []
                for x in (await session.execute(query)).all():
                    gpu_card = dict(x[0]) | dict(x[1])
                    gpu_cards.append(gpu_card)

                if gpu_cards or time_in_s is None:
                    return gpu_cards

            # if here then we fallback to the closest known node configuration to the given
            # timepoint
            subquery = select(
                         SysinfoGpuCardConfig.cluster,
                         SysinfoGpuCardConfig.node,
                         SysinfoGpuCardConfig.uuid,
                         func.min(SysinfoGpuCardConfig.time).label('min_time')
                       ).where(
                            (SysinfoGpuCardConfig.cluster == cluster) &
                            SysinfoGpuCardConfig.node.in_(node) &
                            (SysinfoGpuCardConfig.time >= dt.datetime.fromtimestamp(time_in_s, dt.timezone.utc))
                       ).group_by(
                         SysinfoGpuCardConfig.cluster,
                         SysinfoGpuCardConfig.node,
                         SysinfoGpuCardConfig.uuid
                       ).subquery()

            query = select(
                        SysinfoGpuCard,
                        SysinfoGpuCardConfig,
                    ).join(
                        subquery,
                        (SysinfoGpuCard.uuid == subquery.c.uuid) &
                        (SysinfoGpuCardConfig.cluster == subquery.c.cluster) &
                        (SysinfoGpuCardConfig.node == subquery.c.node) &
                        (SysinfoGpuCardConfig.time == subquery.c.min_time)
                    ).join(
                        SysinfoGpuCard,
                        (SysinfoGpuCard.uuid == SysinfoGpuCardConfig.uuid)
                    ).order_by(None)

            async with self.make_async_session() as session:
                gpu_cards = []
                for x in (await session.execute(query)).all():
                    gpu_card = dict(x[0]) | dict(x[1])
                    gpu_cards.append(gpu_card)
                return gpu_cards


        except Exception as e:
            logger.warning(e)
            raise

        return []


    async def get_gpu_infos(self, cluster: str, node: str,
            time_in_s: int | None = None ,
            ) -> Awaitable[list[dict[str, any]]]:
        """
        Get an array of SysinfoGpuCard information for the given cluster and node
        """
        # model, node, uuid, local_id, memory_total
        gpu_cards = await self.get_active_sysinfo_gpu_card(
                        cluster=cluster,
                        node=node,
                        time_in_s=time_in_s
                    )
        if gpu_cards:
            return [ dict(x) for x in gpu_cards ]
        return {}

    async def get_last_probe_timestamp(self, cluster: str) -> Awaitable[dict[str, dt.datetime]]:
        """
        Get the timestamp of the lastest sample for all nodes in this cluster
        """
        query = select(
                     SampleProcess.node,
                     func.max(SampleProcess.time).label('max_time')
                   ).where(
                     SampleProcess.cluster == cluster
                   ).group_by(
                     SampleProcess.node
                   )

        async with self.make_async_session() as session:
            timestamps = (await session.execute(query)).all()
            if timestamps:
                # result:
                #   list of tuple - (node, max_timestamp)
                #   merge into a single dictionary
                return {x[0]: x[1] for x in timestamps}

        return {}


    async def get_nodes_sample_process_gpu_timeseries(
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
        Get the SampleProcessGpu timeseries for a set of nodes in a cluster.

        return:
            nodename:
                gpu_uuid: Array[{job: int, epoch: int, data: Array[SampleProcessGpu]]
        """
        node_configs = await self.get_active_node_configs(
                           cluster=cluster,
                           node=nodes,
                           time_in_s=start_time_in_s,
                        )

        if not node_configs:
            raise RuntimeError(f"sample_process_gpu: {nodes=} on {cluster=} are not available")

        nodes_sample_process_gpu = {}
        for node_config in node_configs:
            cards = [x[0] for x in node_config.cards]

            where = SampleProcessGpu.uuid.in_(cards)
            if job_id is not None:
                where &= (SampleProcessGpu.job == job_id) & (SampleProcessGpu.epoch == epoch)

            if start_time_in_s:
                where &= SampleProcessGpu.time >= dt.datetime.fromtimestamp(start_time_in_s, dt.timezone.utc)

            if end_time_in_s:
                where &= SampleProcessGpu.time <= dt.datetime.fromtimestamp(end_time_in_s, dt.timezone.utc)

            # Identifiable processes in the time window
            subquery = select(
                        SampleProcessGpu.job,
                        SampleProcessGpu.epoch,
                        SampleProcessGpu.pid,
                        SampleProcessGpu.uuid
                    ).where(
                        where
                    ).distinct()

            async with self.make_async_session() as session:
                process_ids = (await session.execute(subquery)).all()

            processes = {}
            for process_id in process_ids:
                job, epoch, pid, uuid = process_id
                timeseries_data = await self.get_sample_process_gpu_timeseries(
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

            nodes_sample_process_gpu[node_config.node] = processes

        return nodes_sample_process_gpu

    async def get_jobs_sample_process_system_timeseries(
            self,
            cluster: str,
            job_id: int | None,
            epoch: int,
            start_time_in_s: int,
            end_time_in_s: int,
            resolution_in_s: int,
            nodes: list[str] | None = None
            ):

        gpu_status = await self.get_jobs_sample_process_gpu_timeseries(
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
                    SampleProcess.job,
                    SampleProcess.epoch,
                    func.array_agg(SampleProcess.node.distinct())
                ).where(
                    (SampleProcess.cluster == cluster) &
                    (SampleProcess.time >= dt.datetime.fromtimestamp(start_time_in_s, dt.timezone.utc)) &
                    (SampleProcess.time <= dt.datetime.fromtimestamp(end_time_in_s, dt.timezone.utc))
                ).group_by(
                    SampleProcess.job,
                    SampleProcess.epoch
                ).distinct()

        async with self.make_async_session() as session:
            return (await session.execute(query)).all()


    async def get_jobs_sample_process_gpu_timeseries(
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
        Get the SampleProcessGpu timeseries for jobs in a cluster.

        return:
            Array[{job: int, epoch: int, nodes: { node: { uuid:  Array[SampleProcessGpu]}}}]
        """

        where = SampleProcessGpu.cluster == cluster
        if nodes:
            where &= SampleProcessGpu.node.in_(nodes)

        if job_id is not None:
            where &= (SampleProcessGpu.job == job_id) & (SampleProcessGpu.epoch == epoch)

        if start_time_in_s:
            where &= SampleProcessGpu.time >= dt.datetime.fromtimestamp(start_time_in_s, dt.timezone.utc)

        if end_time_in_s:
            where &= SampleProcessGpu.time <= dt.datetime.fromtimestamp(end_time_in_s, dt.timezone.utc)

        query = select(
                    SampleProcessGpu.job,
                    SampleProcessGpu.epoch,
                    func.array_agg(SampleProcessGpu.node.distinct()),
                ).where(
                    where
                ).group_by(
                    SampleProcessGpu.job,
                    SampleProcessGpu.epoch
                ).distinct()

        async with self.make_async_session() as session:
            jobs = (await session.execute(query)).all()


        job_sample_process_gpu = []
        for job in jobs:
            job_id, job_epoch, job_nodes = job
            nodes = {}
            for node in job_nodes:
                # Identifiable processes in the time window
                query = select(
                            SampleProcessGpu.uuid
                        ).where(
                            (SampleProcessGpu.job == job_id) &
                            (SampleProcessGpu.epoch == epoch) &
                            (SampleProcessGpu.node == node)
                        ).distinct()

                async with self.make_async_session() as session:
                    uuids = [x[0] for x in (await session.execute(query)).all()]


                card_data = {}
                for uuid in uuids:
                    card_data[uuid] = await self.get_sample_process_gpu_timeseries(
                        uuid=uuid,
                        job=job_id,
                        epoch=job_epoch,
                        start_time_in_s=start_time_in_s,
                        end_time_in_s=end_time_in_s,
                        resolution_in_s=resolution_in_s
                    )
                nodes[node] = { 'gpus': card_data }
            job_sample_process_gpu.append({'job': job_id, 'epoch': epoch, 'nodes': nodes})
        return job_sample_process_gpu

    async def get_sample_process_gpu_timeseries(self,
            uuid: str,
            job: int,
            epoch: int,
            start_time_in_s: int,
            end_time_in_s: int,
            resolution_in_s: int) -> Awaitable[list[dict[str, any]]]:
        """
        Get the SampleProcessGpu timeseries for a given GPU (by uuid) and a job
        identified by job and epoch

        return
            Array[{ gpu_util: float, gpu_memory: float, gpu_memory_util: float, pids: list[int], time: dt.datetime}]

        """
        query = select(
                    func.avg(SampleProcessGpu.gpu_util),
                    func.avg(SampleProcessGpu.gpu_memory),
                    func.avg(SampleProcessGpu.gpu_memory_util),
                    func.array_agg(SampleProcessGpu.pid.distinct()),
                    time_bucket(resolution_in_s, SampleProcessGpu.time).label('time_bucket')
                ).where(
                    (SampleProcessGpu.job == job) &
                    (SampleProcessGpu.epoch == epoch) &
                    (SampleProcessGpu.uuid == uuid) &
                    (SampleProcessGpu.time >= dt.datetime.fromtimestamp(start_time_in_s, dt.timezone.utc)) &
                    (SampleProcessGpu.time <= dt.datetime.fromtimestamp(end_time_in_s, dt.timezone.utc))
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
                        'time': x[4]
                    } for x in timeseries
            ]

    async def get_node_process_gpu_util(self,
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

        return await self.get_process_gpu_util(uuids=cards,
                reference_time_in_s=reference_time_in_s,
                window_in_s=window_in_s
                )


    async def get_process_gpu_util(self,
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
                         SampleProcessGpu.uuid,
                         func.max(SampleProcessGpu.time).label('max_time')
                       ).where(
                         (SampleProcessGpu.uuid.in_(uuids)) &
                         (SampleProcessGpu.time >= dt.datetime.fromtimestamp(start_time_in_s, dt.timezone.utc))
                       ).group_by(
                         SampleProcessGpu.uuid
                       )

            async with self.make_async_session() as session:
                last_time = (await session.execute(query)).all()

            data = {}
            for last_timestamp in last_timestamps:
                uuid, timestamp = last_timestamp

                query = select(
                            SampleProcessGpu.uuid,
                            SampleProcessGpu.gpu_memory,
                            func.sum(SampleProcessGpu.gpu_util),
                            func.sum(SampleProcessGpu.gpu_memory_util),
                        ).where(
                            (SampleProcessGpu.uuid == uuid) &
                            (SampleProcessGpu.time == timestamp)
                        ).group_by(
                            SampleProcessGpu.uuid,
                            SampleProcessGpu.gpu_memory
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

    async def get_nodes_sample_process_timeseries(
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
            raise RuntimeError(f"sample_process: {nodes=} on {cluster=} are not available")

        nodes_sample_process = {}
        for node_config in node_configs:
            where = (SampleProcess.cluster == cluster) & (SampleProcess.node == node_config.node)
            if start_time_in_s:
                where &= SampleProcess.time >= dt.datetime.fromtimestamp(start_time_in_s, dt.timezone.utc)

            if end_time_in_s:
                where &= SampleProcess.time <= dt.datetime.fromtimestamp(end_time_in_s, dt.timezone.utc)

            # Identifiable processes in the time window
            subquery = select(
                        SampleProcess.job,
                        SampleProcess.epoch,
                        SampleProcess.pid,
                    ).where(
                        where
                    ).distinct()

            subquery = subquery.distinct()

            async with self.make_async_session() as session:
                process_ids = (await session.execute(subquery)).all()

            jobs = {}
            for process_id in process_ids:
                job, epoch, pid = process_id
                timeseries_data = await self.get_sample_process_timeseries(
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

            nodes_sample_process[node_config.node] = [y for x,y in jobs.items()]

        return nodes_sample_process

    async def get_sample_process_timeseries(self,
            job: int,
            epoch: int,
            pid: str,
            memory: int,
            start_time_in_s: int,
            end_time_in_s: int,
            resolution_in_s: int) -> Awaitable[list[dict[str, any]]]:

            query = select(
                        func.avg(SampleProcess.resident_memory),
                        func.avg(SampleProcess.virtual_memory),
                        (func.avg(SampleProcess.resident_memory) / memory).label('memory_util'),
                        func.avg(SampleProcess.cpu_avg),
                        func.avg(SampleProcess.cpu_util),
                        func.avg(SampleProcess.cpu_time),
                        time_bucket(resolution_in_s, SampleProcess.time).label("bucket"),
                    ).where(
                        (SampleProcess.job == job) &
                        (SampleProcess.epoch == epoch) &
                        (SampleProcess.pid == pid)
                    ).filter(
                        SampleProcess.time >= dt.datetime.fromtimestamp(start_time_in_s, dt.timezone.utc)
                    ).filter(
                        SampleProcess.time <= dt.datetime.fromtimestamp(end_time_in_s, dt.timezone.utc)
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
                            'time': x[6]
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
                        SysinfoAttributes.memory,
                        SysinfoAttributes.time
                   ).where(
                        (SysinfoAttributes.cluster == cluster),
                        (SysinfoAttributes.node == node),
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

            where = (SampleProcess.cluster == cluster) & (SampleProcess.node == node)
            if job_id:
                where &= (SampleProcess.job == job_id) & (SampleProcess.epoch == epoch)

            subquery = select(
                        func.sum(SampleProcess.resident_memory).label('resident_memory'),
                        func.sum(SampleProcess.virtual_memory).label('virtual_memory'),
                        func.sum(SampleProcess.cpu_avg).label('cpu_avg'),
                        func.sum(SampleProcess.cpu_util).label('cpu_util'),
                        func.sum(SampleProcess.cpu_time).label('cpu_time'),
                        func.array_agg(func.cast(SampleProcess.pid, Integer).distinct()).label('pids'),
                        SampleProcess.time
                    ).where(
                        where &
                        (SampleProcess.time >= dt.datetime.fromtimestamp(start_time_in_s, dt.timezone.utc)) &
                        (SampleProcess.time <= dt.datetime.fromtimestamp(end_time_in_s, dt.timezone.utc))
                    ).group_by(
                       SampleProcess.time,
                    ).order_by(
                       SampleProcess.time.asc()
                    )

            subquery = subquery.subquery()

            query = select(
                        func.avg(subquery.c.resident_memory),
                        func.avg(subquery.c.virtual_memory),
                        (func.avg(subquery.c.resident_memory)/memory).label("memory_util"),
                        func.avg(subquery.c.cpu_avg),
                        func.avg(subquery.c.cpu_util),
                        func.avg(subquery.c.cpu_time),
                        func.avg(func.distinct(func.array_length(subquery.c.pids, 1))),
                        time_bucket(resolution_in_s, subquery.c.time).label("bucket"),
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
                            'time': x[7]
                        } for x in timeseries
                ]

    async def get_sample_gpu_timeseries(
            self,
            cluster: str,
            node: str,
            start_time_in_s: int,
            end_time_in_s: int,
            resolution_in_s: int
            ):
        """
        Get the latest GPU sample data for a given timeframe.
        For all cards on a node:
        return:
            [ { uuid: string, local_index: string, data: Array[SampleProcessGpu]} ]
        """
        query = select(
                SysinfoAttributes.cluster,
                SysinfoAttributes.node,
                SysinfoAttributes.cards,
                func.max(SysinfoAttributes.time)
               ).where(
                    (SysinfoAttributes.cluster == cluster),
                    (SysinfoAttributes.node == node)
               ).group_by(
                   SysinfoAttributes.cluster,
                   SysinfoAttributes.node,
                   SysinfoAttributes.cards
               )

        async with self.make_async_session() as session:
            node_configs = (await session.execute(query)).all()

        gpu_sample = []
        for node_config in node_configs:
            cards = [x[0] for x in node_config[2]]

            for card_uuid in cards:

                where = SampleGpu.uuid == card_uuid
                if start_time_in_s:
                    where &= (SampleGpu.time >= dt.datetime.fromtimestamp(start_time_in_s, dt.timezone.utc))

                if end_time_in_s:
                    where &= (SampleGpu.time <= dt.datetime.fromtimestamp(end_time_in_s, dt.timezone.utc))

                query = select(
                        func.count(SampleGpu.failing).label("failure_count"),
                        func.avg(SampleGpu.memory),
                        func.avg(SampleGpu.memory_util),
                        func.avg(SampleGpu.ce_util),
                        func.avg(SampleGpu.temperature),
                        func.avg(SampleGpu.power),
                        func.avg(SampleGpu.power_limit),
                        func.avg(SampleGpu.memory_clock),
                        func.min(SampleGpu.index),
                        time_bucket(resolution_in_s, SampleGpu.time).label('bucket')
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
                                'time': x[9]
                            } for x in node_samples]

                        gpu_sample.append({'uuid': card_uuid, 'local_index': local_index, 'data': timeseries_data })

        return gpu_sample


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

    async def get_job(
            self,
            cluster: str,
            job_id: int,
            epoch: int,
            start_time_in_s: int | None = None,
            end_time_in_s: int | None = None,
            states: list[str] | None = None,
        ) -> dict[str, any] | None:

        if epoch == 0:
            return await self.get_slurm_job(
                    cluster,
                    job_id=job_id,
                    start_time_in_s=start_time_in_s,
                    end_time_in_s=end_time_in_s,
                    states=states)
        else:
            raise NotImplementedError("get_job not yet implemented")


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
        where = (SampleSlurmJob.cluster == cluster) & (SampleSlurmJob.job_id == job_id) & (SampleSlurmJob.job_step == '') # get the main job
        if start_time_in_s:
            where &= (SampleSlurmJob.time >= dt.datetime.fromtimestamp(start_time_in_s, dt.timezone.utc))
        if end_time_in_s:
            where &= (SampleSlurmJob.time <= dt.datetime.fromtimestamp(end_time_in_s, dt.timezone.utc))

        query = select(
                    SampleSlurmJob
                ).where(
                    where
                ).order_by(
                    SampleSlurmJob.time.desc()
                ).limit(1)

        gpus_query = select(
                    SampleProcessGpu.uuid.distinct()
                ).where(
                    (SampleProcessGpu.cluster == cluster) &
                    (SampleProcessGpu.job == job_id) &
                    (SampleProcessGpu.epoch == 0)
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
            where &= SampleSlurmJob.user_name == user

        if user_id:
            where &= SampleSlurmJob.user_id == user_id

        if job_id:
            where &= SampleSlurmJob.job_id == job_id

        if start_before_in_s is not None:
            reference_time = dt.datetime.fromtimestamp(start_before_in_s, dt.timezone.utc).replace(tzinfo=None)
            where &= SampleSlurmJob.start_time <= reference_time

        if start_after_in_s is not None:
            reference_time = dt.datetime.fromtimestamp(start_after_in_s, dt.timezone.utc).replace(tzinfo=None)
            where &= SampleSlurmJob.start_time >= reference_time

        if end_before_in_s is not None:
            reference_time = dt.datetime.fromtimestamp(end_before_in_s, dt.timezone.utc).replace(tzinfo=None)
            where &= SampleSlurmJob.end_time <= reference_time

        if end_after_in_s is not None:
            reference_time = dt.datetime.fromtimestamp(end_after_in_s, dt.timezone.utc).replace(tzinfo=None)
            where &= SampleSlurmJob.end_time >= reference_time

        if submit_before_in_s is not None:
            reference_time = dt.datetime.fromtimestamp(submit_before_in_s, dt.timezone.utc).replace(tzinfo=None)
            where &= SampleSlurmJob.submit_time <= reference_time

        if submit_after_in_s is not None:
            reference_time = dt.datetime.fromtimestamp(submit_after_in_s, dt.timezone.utc).replace(tzinfo=None)
            where &= SampleSlurmJob.submit_time >= reference_time

        if min_duration_in_s is not None:
            where &= (EpochFn(SampleSlurmJob.end_time) - EpochFn(SampleSlurmJob.start_time)) >= min_duration_in_s

        if max_duration_in_s is not None:
            where &= (EpochFn(SampleSlurmJob.end_time) - EpochFn(SampleSlurmJob.start_time)) <= max_duration_in_s

        # limit search to completed jobs
        where &= (SampleSlurmJob.job_state == 'COMPLETED') & (SampleSlurmJob.user_name != '')
        subquery = select(
                    SampleSlurmJob.job_id.label('job_id'),
                    func.max(SampleSlurmJob.time).label('last_timestamp')
                ).where(
                    where &
                    (SampleSlurmJob.user_name != '')
                ).group_by(
                    SampleSlurmJob.job_id
                ).order_by(None).subquery()

        query = select(
                    SampleSlurmJob
                ).where(
                    (SampleSlurmJob.user_name != '') &
                    (SampleSlurmJob.job_id == subquery.c.job_id) &
                    (SampleSlurmJob.time == subquery.c.last_timestamp)
                )

        async with self.make_async_session() as session:
            return [dict(x[0]) for x in (await session.execute(query)).all()]


    async def get_slurm_jobs(
            self,
            cluster: str,
            partition: str | None = None,
            job_states: list[str] | None = None,
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

        where = (SampleSlurmJob.cluster == cluster)
        where &= (SampleSlurmJob.time >= dt.datetime.fromtimestamp(start_time_in_s, dt.timezone.utc))
        where &= (SampleSlurmJob.time <= dt.datetime.fromtimestamp(end_time_in_s, dt.timezone.utc))

        if job_states:
            where &= SampleSlurmJob.job_state.in_(job_states)

        #if partition:
        #    where &= (SampleSlurmJob.partition == partition)

        subquery = select(
                SampleSlurmJob.job_id,
                SampleSlurmJob.job_step,
                func.max(SampleSlurmJob.time).label("timestamp")
            ).where(
                where
            ).group_by(
                SampleSlurmJob.job_id,
                SampleSlurmJob.job_step
            ).subquery()

        query = select(
                    SampleSlurmJob,
                    SampleSlurmJobAcc
                ).join(subquery,
                    (subquery.c.job_id == SampleSlurmJob.job_id) &
                    (subquery.c.job_step == SampleSlurmJob.job_step) &
                    (subquery.c.timestamp == SampleSlurmJob.time)
                ).join(SampleSlurmJobAcc,
                    (SampleSlurmJob.cluster == cluster) &
                    (SampleSlurmJob.job_id == SampleSlurmJobAcc.job_id) &
                    (SampleSlurmJob.job_step == SampleSlurmJobAcc.job_step) &
                    (SampleSlurmJob.time == SampleSlurmJobAcc.time)
                )

        async with self.make_async_session() as session:
            data = (await session.execute(query)).all()

            samples = []
            for sample in data:
                new_sample = dict(sample[0])
                #for i in ["start_time", "submit_time", "end_time"]:
                #    if (s := new_sample[i]):
                #        new_sample[i] = s.isoformat()

                if sample[1]:
                    new_sample['sacct'] =  dict(sample[1])
                    del new_sample['sacct']['job_id']
                    del new_sample['sacct']['job_step']
                    del new_sample['sacct']['time']

                samples.append(new_sample)

            return samples

    
    async def get_partitions(self, cluster: str,
            time_in_s: int | None = None):

        if time_in_s is None:
            time_in_s = utcnow().timestamp()


        query = select(
                    Partition.cluster,
                    Partition.partition,
                    Partition.nodes,
                    Partition.nodes_compact,
                    func.max(Partition.time)
                ).where(
                    Partition.time <= dt.datetime.fromtimestamp(time_in_s, dt.timezone.utc)
                ).group_by(
                    Partition.cluster,
                    Partition.partition,
                    Partition.nodes,
                    Partition.nodes_compact
                )

        nodes = await self.get_nodes_info(cluster=cluster, time_in_s=time_in_s)

        partitions = []
        async with self.make_async_session() as session:
            for x in (await session.execute(query)).all():
                partition_name = x[1]
                partition_nodes = x[2]

                pending_jobs = await self.get_slurm_jobs(
                        cluster=cluster,
                        partition=partition_name,
                        job_states=["PENDING"],
                        start_time_in_s= time_in_s - 5*60,
                        end_time_in_s= time_in_s + 5*60,
                )

                running_jobs = await self.get_slurm_jobs(
                        cluster=cluster,
                        partition=partition_name,
                        job_states=["RUNNING"],
                        start_time_in_s=time_in_s - 5*60,
                        end_time_in_s=time_in_s + 5*60,
                )

                # logical cpus
                total_cpus = sum([y['cores_per_socket']*y['sockets']*y['threads_per_core'] 
                    for x,y in nodes.items() if x in partition_nodes])

                pending_max_submit_time = 0
                if pending_jobs:
                    pending_max_submit_time = max([x['submit_time'] for x in pending_jobs])
                # for the latest running job -> wait time
                wait_time = 0
                latest_start_time = None
                for job in running_jobs:
                    start_time = job['start_time'] 
                    if latest_start_time is None:
                        latest_start_time = start_time
                        wait_time = start_time - job['submit_time']
                    elif start_time < latest_start_time:
                        latest_start_time = start_time
                        wait_time = start_time - job['submit_time']

                partitions.append({
                    'cluster': x[0],
                    'name': partition_name,
                    'nodes': partition_nodes,
                    'nodes_compact': x[3],
                    'jobs_running': pending_jobs,
                    'jobs_pending': running_jobs,
                    'pending_max_submit_time': pending_max_submit_time,
                    'running_latest_wait_time': wait_time,
                    'total_cpus': total_cpus,
                    'time': x[4],
                })
        partitions.sort(key=lambda x: x['name'])
        return partitions

