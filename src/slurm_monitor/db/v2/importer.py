import datetime as dt
import logging
import sqlalchemy
import traceback as tb

from slurm_monitor.utils import utcnow
from slurm_monitor.utils.slurm import Slurm
from slurm_monitor.db.v2.db_tables import (
    Cluster,
    ErrorMessage,
    Node,
    NodeState,
    Partition,
    SampleDisk,
    SampleGpu,
    SampleProcess,
    SampleProcessGpu,
    SampleSlurmJob,
    SampleSlurmJobAcc,
    SampleSystem,
    SysinfoAttributes,
    SysinfoGpuCard,
    SysinfoGpuCardConfig,
    TableBase,
)
from slurm_monitor.db.v2.db import ClusterDB

import slurm_monitor.db.v2.sonar as sonar

logger = logging.getLogger(__name__)


class Importer:
    last_msg_per_node: dict[str, dt.datetime]
    verbose: bool

    def __init__(self):
        self.last_msg_per_node = {}
        self.verbose = False

    async def insert(
        self, message: sonar.Message, update: bool, ignore_integrity_errors: bool
    ):
        msg = self.to_message(message)
        attributes = msg.data.attributes

        if "node" in attributes:
            node = attributes["node"]
            time = dt.datetime.fromisoformat(attributes["time"])
            self.update_last_msg(node, time)

    def update_last_msg(self, node: str, time: dt.datetime):
        self.last_msg_per_node[node] = time

    async def autoupdate(self, cluster: str):
        pass

    @classmethod
    def to_message(cls, message: dict[str, any]) -> sonar.Message:
        if "meta" not in message:
            raise ValueError(f"Missing 'meta' in {message=}")

        meta = sonar.Meta(**message["meta"])

        data = None
        if "data" not in message and "errors" not in message:
            raise ValueError(f"Either 'data' or 'errors' must be present in {message=}")

        if "data" in message:
            data = sonar.Data(**message["data"])

        errors = None
        if "errors" in message:
            errors = [ErrorMessage(**x) for x in message["errors"]]

        return sonar.Message(meta=meta, data=data, errors=errors)


class DBJsonImporter(Importer):
    db: ClusterDB

    # Check if the the gpu is known and whether the information is 'complete',
    # i.e., not just a stub entry
    sysinfo_gpu_cards: dict[str, SysinfoGpuCard]

    def __init__(self, db: ClusterDB):
        super().__init__()
        self.db = db
        self.sysinfo_gpu_cards = {}

    def ensure_node(self, cluster: str, node: str, rows: list[TableBase]):
        if self.db.is_known_node(cluster=cluster, node=node):
            return rows
        else:
            logger.info(f"Creating node: {cluster=} {node=}")
            return [Node.create(cluster=cluster, node=node)] + rows

    async def sync_with_db(self):
        await self.update_sysinfo_gpu_cards()

    async def autoupdate(self, cluster: str):
        await self.db.sync_cluster_and_nodes_with_jobs(cluster=cluster)

    async def update_sysinfo_gpu_cards(self):
        sysinfo_gpu_cards = await self.db.get_all_sysinfo_gpu_cards()
        self.sysinfo_gpu_cards = {x.uuid: x for x in sysinfo_gpu_cards}

    def ensure_gpu(self, cluster: str, node: str, uuid: str, rows: list[TableBase]):
        if uuid in self.sysinfo_gpu_cards:
            return rows
        else:
            logger.info(f"Creating sysinfo_gpu_card: {cluster=} {node=}")
            return [
                SysinfoGpuCard.create(
                    uuid=uuid, manufacturer="", model="", architecture="", memory=0
                )
            ] + rows

    def parse(self, message: dict[str, any]):
        """
        Parse a message a trigger parsing according to the respective message type

        Will call DBJsonImporter.parse_<msgtype> function to handle a message.
        """
        msg = self.to_message(message)

        msg_type = "errors"
        if not msg.errors:
            msg_type = msg.data.type

        if not hasattr(self, f"parse_{msg_type}"):
            raise NotImplementedError(
                f"DBJsonImporter.parse: no parser for message type: {msg_type} implemented"
            )

        parser_fn = getattr(self, f"parse_{msg_type}")
        return parser_fn(msg)

    def parse_sysinfo(self, msg: sonar.Message) -> list[TableBase | list[TableBase]]:
        """
        Parse messages of type 'sysinfo'
        """
        attributes = msg.data.attributes
        cluster = attributes.get("cluster", "")
        node = attributes["node"]

        time = dt.datetime.fromisoformat(attributes["time"])
        del attributes["time"]

        self.update_last_msg(node=node, time=time)

        gpu_uuids = []
        gpu_info = []
        if "cards" in attributes:
            cards = attributes["cards"]
            del attributes["cards"]

            gpu_cards = []
            gpu_card_configs = []

            for card in cards:
                data = {}
                for field in ["manufacturer", "model", "architecture", "memory"]:
                    if field in card:
                        data[field] = card[field]
                        del card[field]

                gpu_uuid = card["uuid"]
                if gpu_uuid is None or gpu_uuid == "":
                    logger.debug(
                        f"SysInfo: skipping message from {cluster=} {node=} due to missing 'uuid' {card=}"
                    )
                    continue

                gpu_cards.append(
                    SysinfoGpuCard.create(
                        uuid=gpu_uuid,
                        **data,
                    )
                )

                gpu_card_configs.append(
                    SysinfoGpuCardConfig.create(
                        cluster=cluster, node=node, time=time, **card
                    )
                )
                gpu_uuids.append(gpu_uuid)

            gpu_info.append(gpu_cards)
            gpu_info.append(gpu_card_configs)

        node = Node.create(
            cluster=attributes.get("cluster", ""),
            node=attributes["node"],
            architecture=attributes["architecture"],
        )

        sysinfo = SysinfoAttributes.create(time=time, cards=gpu_uuids, **attributes)
        return [node, sysinfo] + gpu_info

    def parse_sample(self, msg: sonar.Message) -> list[TableBase | list[TableBase]]:
        """
        Parse messages of type 'sample'
        """
        attributes = msg.data.attributes
        time = dt.datetime.fromisoformat(attributes["time"])
        del attributes["time"]

        cluster = attributes.get("cluster", "")
        node = attributes["node"]

        self.update_last_msg(node=node, time=time)

        disk_samples = []
        gpu_samples = []
        sample_system = None

        system = attributes.get("system", {})

        active_gpus = set()
        if system:
            if "disks" in system:
                disks = system["disks"]
                del system["disks"]

                for disk in disks:
                    disk_sample = SampleDisk.create(
                        cluster=cluster, node=node, **disk, time=time
                    )
                    disk_samples.append(disk_sample)

            if "gpus" in system:
                gpus = system["gpus"]
                del system["gpus"]

                for gpu in gpus:
                    if "uuid" not in gpu:
                        logger.debug(
                            f"Sample: skipping sample due to missing 'uuid' {gpu=}"
                        )
                        continue

                    active_gpus.add(gpu["uuid"])

                    gpu_status = SampleGpu.create(**gpu, time=time)
                    gpu_samples.append(gpu_status)

            # consume remaining items from SampleSystem
            sample_system = SampleSystem.create(
                cluster=cluster, node=node, **system, time=time
            )

        process_stati = []
        gpu_card_process_stati = []
        jobs = attributes.get("jobs", [])
        for job in jobs:
            job_id = job["job"]

            user = job["user"]
            epoch = job.get("epoch", 0)

            for process in job["processes"]:
                if "pid" not in process:
                    process["pid"] = 0

                pid = process["pid"]

                if "gpus" in process:
                    for gpu_data in process["gpus"]:
                        active_gpus.add(gpu_data["uuid"])

                        gpu_card_process_stati.append(
                            SampleProcessGpu.create(
                                cluster=cluster,
                                node=node,
                                pid=pid,
                                job=job_id,
                                user=user,
                                epoch=epoch,
                                time=time,
                                **gpu_data,
                            )
                        )
                    del process["gpus"]

                process_stati.append(
                    SampleProcess.create(
                        cluster=cluster,
                        node=node,
                        job=job_id,
                        user=user,
                        epoch=epoch,
                        time=time,
                        **process,
                    )
                )
        rows = self.ensure_node(
            cluster=cluster,
            node=node,
            rows=[
                disk_samples,
                gpu_samples,
                gpu_card_process_stati,
                process_stati,
                sample_system,
            ],
        )
        for uuid in active_gpus:
            rows = self.ensure_gpu(cluster=cluster, node=node, uuid=uuid, rows=rows)

        return rows

    def parse_cluster(self, msg: sonar.Message) -> list[TableBase | list[TableBase]]:
        """
        Parse messages of type 'cluster'
        """
        attributes = msg.data.attributes
        cluster_id = attributes.get("cluster", "")
        slurm = attributes["slurm"]

        time = dt.datetime.fromisoformat(attributes["time"])
        del attributes["time"]

        nodes = set()
        nodes_states = []
        for n in attributes["nodes"]:
            for names in n["names"]:
                node_names = Slurm.expand_node_names(names)
                nodes.update(node_names)

            states = n["states"]
            for n in node_names:
                nodes_states.append(
                    NodeState.create(
                        cluster=cluster_id, node=n, states=states, time=time
                    )
                )

        partitions = []
        for p in attributes["partitions"]:
            partition_nodes = set()
            for names in p["nodes"]:
                node_names = Slurm.expand_node_names(names)
                partition_nodes.update(node_names)

            partitions.append(
                Partition.create(
                    cluster=cluster_id,
                    partition=p["name"],
                    nodes=node_names,
                    nodes_compact=p["nodes"],
                    time=time,
                )
            )

        cluster = Cluster.create(
            cluster=cluster_id,
            slurm=slurm,
            partitions=[x.partition for x in partitions],
            nodes=list(nodes),
            time=time,
        )

        cluster_nodes = [Node.create(cluster=cluster_id, node=x) for x in nodes]

        return [cluster] + partitions + cluster_nodes + nodes_states

    def parse_job(self, msg: sonar.Message) -> list[TableBase | list[TableBase]]:
        """
        Parse messages of type 'job'
        """
        attributes = msg.data.attributes
        cluster = attributes.get("cluster", "")
        slurm_jobs = attributes["slurm_jobs"]

        time = dt.datetime.fromisoformat(attributes["time"])
        del attributes["time"]

        slurm_job_samples = []
        for job_data in slurm_jobs:
            sacct = None
            if "sacct" in job_data:
                sacct = job_data["sacct"]
                del job_data["sacct"]

            slurm_job_samples.append(
                SampleSlurmJob.create(cluster=cluster, **job_data, time=time)
            )
            if sacct:
                if "job_step" in job_data:
                    sacct["job_step"] = job_data["job_step"]

                slurm_job_samples.append(
                    SampleSlurmJobAcc.create(
                        cluster=cluster, job_id=job_data["job_id"], **sacct, time=time
                    )
                )
        return slurm_job_samples

    def parse_errors(self, msg: sonar.Message) -> list[TableBase | list[TableBase]]:
        """
        Parse messages of type 'errors'
        """
        error_messages = []
        for error in msg.errors:
            error_messages.append(ErrorMessage.create(**error))
        return error_messages

    async def insert(
        self,
        message: dict[str, any],
        update: bool = True,
        ignore_integrity_errors: bool = False,
    ):
        # sync with the current db state once before handling all samples in a message
        await self.sync_with_db()

        rows = self.parse(message)
        for row in rows:
            if row:
                try:
                    if update:
                        self.db.insert_or_update(row)
                    else:
                        self.db.insert(row)

                    if type(row) is Node:
                        nodes = await self.db.get_nodes(
                            cluster=row.cluster, ensure_sysinfo=False
                        )
                        if row.node not in nodes:
                            updated_nodes = set(nodes)
                            updated_nodes.add(row.node)

                            # Update the associated cluster at the same time
                            cluster = Cluster.create(
                                cluster=row.cluster,
                                slurm=False,
                                partitions=[],
                                nodes=updated_nodes,
                                time=utcnow(),
                            )
                            self.db.insert(cluster)
                except Exception as e:
                    if (
                        ignore_integrity_errors
                        and type(e) is sqlalchemy.exc.IntegrityError
                    ):
                        continue
                    else:
                        if self.verbose:
                            tb.print_tb(e.__traceback__)
                        logger.warning(f"Inserting {row=} failed. -- {e}")
