import logging
import re
import json
import datetime as dt
import sqlalchemy
import traceback as tb

from slurm_monitor.utils import utcnow
from slurm_monitor.db.v2.db_tables import (
    Cluster,
    ErrorMessage,
    Node,
    NodeState,
    Partition,
    SampleGpu,
    SampleProcess,
    SampleProcessGpu,
    SampleSlurmJob,
    SampleSlurmJobAcc,
    SampleSystem,
    SysinfoAttributes,
    SysinfoGpuCard,
    SysinfoGpuCardConfig,
    TableBase
)
from slurm_monitor.db.v2.db import (
    ClusterDB
)

import slurm_monitor.db.v2.sonar as sonar

logger = logging.getLogger(__name__)

class Importer:
    last_msg_per_node: dict[str, dt.datetime]
    verbose: bool

    def __init__(self):
        self.last_msg_per_node = {}
        self.verbose = False

    def insert(message: sonar.Message, update: bool):
        raise NotImplementedError("Importer: insert not implemented")

    def update_last_msg(self, node: str, time: dt.datetime):
        self.last_msg_per_node[node] = time

    @classmethod
    def expand_node_names(cls, names: str) -> list[str]:
        nodes = []

        if type(names) is str:
            names = names.replace("],","];")
            names = names.split(';')

        for pattern in names:
            m = re.match(r"(.*)\[(.*)\]$", pattern)
            if m is None:
                nodes.append(pattern)
                continue

            prefix, suffixes = m.groups()
            # [005-006,001,005]
            for suffix in suffixes.split(','):
                #0,0
                if "-" not in suffix:
                    nodes.append(prefix + suffix)
                    continue

                # 001-010
                start, end = suffix.split("-")
                pattern_length = len(start)

                for i in range(int(start), int(end)+1):
                    node_number = str(i).zfill(pattern_length)
                    nodes.append(prefix + node_number)

        return nodes


class DBJsonImporter(Importer):
    db: ClusterDB
    cluster_nodes: dict[str, set]

    def __init__(self, db: ClusterDB):
        super().__init__()
        self.db = db

    def ensure_node(self, cluster: str, node: str, samples: list[TableBase]):
        if self.db.is_known_node(cluster=cluster, node=node):
            return samples
        else:
            logger.info(f"Creating node: {cluster=} {node=}")
            return [Node.create(cluster=cluster, node=node)] + samples

    @classmethod
    def to_message(cls, message: dict[str, any]) -> sonar.Message:
        if "meta" not in message:
            raise ValueError(f"Missing 'meta' in {message=}")

        meta = sonar.Meta(**message['meta'])

        data = None
        if "data" not in message and "errors" not in message:
            raise ValueError(f"Either 'data' or 'errors' must be present in {message=}")

        if 'data' in message:
            data = sonar.Data(**message['data'])


        errors = None
        if 'errors' in message:
            errors = [ErrorMessage(**x) for x in message['errors']]

        return sonar.Message(meta=meta, data=data, errors=errors)

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
            raise NotImplementedError(f"DBJsonImporter.parse: no parser for message type: {msg_type} implemented")

        parser_fn = getattr(self, f"parse_{msg_type}")
        return parser_fn(msg)


    def parse_sysinfo(self, msg: sonar.Message) -> list[TableBase | list[TableBase]]:
        """
        Parse messages of type 'sysinfo'
        """
        attributes = msg.data.attributes
        cluster = attributes.get('cluster', '')
        node = attributes['node']

        time = dt.datetime.fromisoformat(attributes["time"])
        del attributes['time']

        self.update_last_msg(node=node, time=time)

        gpu_uuids = []
        gpu_info = []
        if 'cards' in attributes:

            cards = attributes['cards']
            del attributes['cards']

            gpu_cards = []
            gpu_card_configs = []

            for card in cards:
                data = {}
                for field in ["manufacturer", "model", "architecture", "memory"]:
                    if field in card:
                        data[field] = card[field]
                        del card[field]

                gpu_uuid = card['uuid']
                if gpu_uuid is None or gpu_uuid == '':
                    logger.debug(f"SysInfo: skipping sample from {cluster=} {node=} due to missing 'uuid' {card=}")
                    continue

                gpu_cards.append(SysinfoGpuCard.create(
                        uuid=gpu_uuid,
                        **data,
                    )
                )

                gpu_card_configs.append(SysinfoGpuCardConfig.create(
                        cluster=cluster,
                        node=node,
                        time=time,
                        **card
                    )
                )
                gpu_uuids.append(gpu_uuid)

            gpu_info.append(gpu_cards)
            gpu_info.append(gpu_card_configs)

        node = Node.create(
                cluster=attributes.get('cluster', ''),
                node=attributes['node'],
                architecture=attributes['architecture']
        )

        sysinfo = SysinfoAttributes.create(
                    time=time,
                    cards=gpu_uuids,
                    **attributes
                )

        return [node, sysinfo] + gpu_info

    def parse_sample(self, msg: sonar.Message) -> list[TableBase | list[TableBase]]:
        """
        Parse messages of type 'sample'
        """
        attributes = msg.data.attributes
        time = dt.datetime.fromisoformat(attributes["time"])
        del attributes['time']

        cluster = attributes.get('cluster','')
        node = attributes['node']

        self.update_last_msg(node=node, time=time)

        gpu_samples = []
        sample_system = None

        system = attributes.get("system",{})
        if system:
            if "gpus" in system:
                gpus = system["gpus"]
                del system["gpus"]

                for gpu in gpus:
                    if 'uuid' not in gpu:
                        logger.debug(f"Sample: skipping sample due to missing 'uuid' {gpu=}")
                        continue

                    gpu_status = SampleGpu.create(
                            **gpu,
                            time=time
                    )
                    gpu_samples.append(gpu_status)

            # consume remaining items from SampleSystem
            sample_system = SampleSystem.create(
                    cluster=cluster,
                    node=node,
                    **system,
                    time=time
            )

        process_stati = []
        gpu_card_process_stati = []
        jobs = attributes.get("jobs", [])
        for job in jobs:
            job_id = job['job']

            user = job['user']
            epoch = job.get('epoch', 0)

            for process in job["processes"]:
                pid = process['pid']

                if "gpus" in process:
                    for gpu_data in process['gpus']:
                        gpu_card_process_stati.append(
                            SampleProcessGpu.create(
                                cluster=cluster,
                                node=node,
                                pid=pid,
                                job=job_id,
                                user=user,
                                epoch=epoch,
                                time=time,
                                **gpu_data
                            )
                        )
                    del process["gpus"]

                process_stati.append( SampleProcess.create(
                   cluster=cluster,
                   node=node,
                   job=job_id,
                   user=user,
                   epoch=epoch,
                   time=time,

                   **process)
                )
        return self.ensure_node(cluster, node, [gpu_samples, gpu_card_process_stati, process_stati, sample_system])

    def parse_cluster(self, msg: sonar.Message) -> list[TableBase | list[TableBase]]:
        """
        Parse messages of type 'cluster'
        """
        attributes = msg.data.attributes
        cluster_id = attributes.get('cluster', '')
        slurm = attributes['slurm']

        time = dt.datetime.fromisoformat(attributes["time"])
        del attributes['time']

        nodes = set()
        nodes_states = []
        for n in attributes['nodes']:
            node_names = self.expand_node_names(n['names'])
            nodes.update(node_names)

            states = n['states']
            for n in node_names:
                nodes_states.append(NodeState.create(
                        cluster=cluster_id,
                        node=n,
                        states=states,
                        time=time
                    )
                )

        partitions = []
        for p in attributes['partitions']:
            node_names = self.expand_node_names(p["nodes"])
            partitions.append(Partition.create(
                    cluster=cluster_id,
                    partition=p["name"],
                    nodes=node_names,
                    nodes_compact=p["nodes"],
                    time=time
                )
            )

        cluster = Cluster.create(
            cluster=cluster_id,
            slurm=slurm,
            partitions=[x.partition for x in partitions],
            nodes=list(nodes),
            time=time
        )

        cluster_nodes = [Node.create(cluster=cluster_id, node=x) for x in nodes]

        return [ cluster ]  + partitions + cluster_nodes + nodes_states

    def parse_job(self, msg: sonar.Message) -> list[TableBase | list[TableBase]]:
        """
        Parse messages of type 'job'
        """
        attributes = msg.data.attributes
        cluster = attributes.get('cluster', '')
        slurm_jobs = attributes['slurm_jobs']

        time = dt.datetime.fromisoformat(attributes["time"])
        del attributes['time']


        slurm_job_samples = []
        for job_data in slurm_jobs:
            sacct = None
            if 'sacct' in job_data:
                sacct = job_data['sacct']
                del job_data['sacct']

            slurm_job_samples.append(
                SampleSlurmJob.create(
                    cluster=cluster,
                    **job_data,
                    time=time
                )
            )
            if sacct:
                if 'job_step' in job_data:
                    sacct['job_step'] = job_data['job_step']

                slurm_job_samples.append(
                    SampleSlurmJobAcc.create(
                        cluster=cluster,
                        job_id=job_data['job_id'],
                        **sacct,
                        time=time
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

    async def insert(self,
                     message: dict[str, any],
                     update: bool = True,
                     ignore_integrity_errors: bool = False):
        samples = self.parse(message)
        for sample in samples:
            if sample:
                try:
                    if update:
                        self.db.insert_or_update(sample)
                    else:
                        self.db.insert(sample)

                    if type(sample) is Node:
                        nodes = await self.db.get_nodes(cluster=sample.cluster, ensure_sysinfo=False)
                        if sample.node not in nodes:
                            updated_nodes = set(nodes)
                            updated_nodes.add(sample.node)

                            # Update the associated cluster at the same time
                            cluster = Cluster.create(
                                cluster=sample.cluster,
                                slurm=False,
                                partitions=[],
                                nodes=updated_nodes,
                                time=utcnow()
                            )
                            self.db.insert(cluster)
                except Exception as e:
                    if ignore_integrity_errors and type(e) is sqlalchemy.exc.IntegrityError:
                        continue
                    else:
                        if self.verbose:
                            tb.print_tb(e.__traceback__)
                        logger.warning(f"Inserting sample {sample} failed. -- {e}")

