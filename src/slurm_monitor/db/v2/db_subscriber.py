from slurm_monitor.db.v2.db_tables import (
    GPUCard,
    GPUCardConfig,
    GPUCardStatus,
    GPUCardProcessStatus,
    Node,
    NodeConfig,
    ProcessStatus,
    SlurmJobStatus,
    SoftwareVersion,
    TableBase
)
from slurm_monitor.db.v2.db import (
    DatabaseSettings,
    ClusterDB
)

import json
import dataclasses
from datetime import datetime as dt


@dataclasses.dataclass
class Meta:
    producer: str
    version: str

@dataclasses.dataclass
class Data:
    type: str
    attributes: dict[str, any]

@dataclasses.dataclass
class Message:
    meta: Meta
    data: Data


class DBJsonImporter:

    db: ClusterDB
    def __init__(self, db: ClusterDB):
        self.db = db

    @classmethod
    def to_message(cls, message: dict[str, any]) -> Message:
        meta = Meta(**message['meta'])
        data = Data(**message['data'])

        return Message(meta=meta, data=data)

    @classmethod
    def parse(cls, message: dict[str, any]):
        msg = DBJsonImporter.to_message(message)
        msg_type = msg.data.type
        if not hasattr(cls, f"parse_{msg_type}"):
            raise NotImplementedError(f"No parser for message type: {msg_type} implemented")

        parser_fn = getattr(cls, f"parse_{msg_type}")
        return parser_fn(msg)


    @classmethod
    def parse_sysinfo(cls, msg: Message) -> list[TableBase | list[TableBase]]:
        attributes = msg.data.attributes
        cluster = attributes.get('cluster', '')
        node = attributes['node']

        timestamp = dt.fromisoformat(attributes["time"])
        del attributes['time']

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

                gpu_uuid = card['uuid'],
                gpu_cards.append(GPUCard(
                        uuid=gpu_uuid,
                        **data
                    )
                )
                gpu_uuids.append(gpu_uuid)

                gpu_card_configs.append(
                        GPUCardConfig(
                            cluster=cluster,
                            node=node,
                            timestamp=timestamp, 
                            **card
                        )
                )
            gpu_info.append(gpu_cards)
            gpu_info.append(gpu_card_configs)
       
        node = Node(
                    cluster=attributes.get('cluster', ''),
                    node=attributes['node']
               )
        node_config = NodeConfig(
                    timestamp=timestamp,
                    cards=gpu_uuids,
                    **attributes
                )

        return [node, node_config] + gpu_info

    @classmethod
    def parse_sample(cls, msg: Message) -> list[TableBase | list[TableBase]]:
        attributes = msg.data.attributes
        timestamp = dt.fromisoformat(attributes["time"])
        del attributes['time']

        system = attributes["system"]
        used_memory = system["used_memory"]
        cpus = system["cpus"]

        gpu_samples = []
        if "gpus" in system:
            gpus = system["gpus"]

            for gpu in gpus:
                gpu_status = GPUCardStatus(**gpu)
                gpu_samples.append(gpu_status)

        cluster = attributes.get('cluster','')
        node = attributes['node']

        jobs = attributes["jobs"]
        process_stati = []
        gpu_card_process_stati = []
        for job in jobs:
            job_id = job['job']

            user = job['user']
            epoch = job.get('epoch', 0)

            for process in job["processes"]:
                pid = process['pid']

                if "gpus" in process:
                    for gpu_data in process['gpus']:
                        gpu_card_process_stati.append(
                            GPUCardProcessStatus(
                                pid=pid,
                                job=job_id,
                                user=user,
                                epoch=epoch,
                                timestamp=timestamp,
                                **gpu_data
                            )
                        )
                    del process["gpus"]

                process_stati.append( ProcessStatus(
                   cluster=cluster,
                   node=node,
                   job=job_id,
                   user=user,
                   epoch=epoch,
                   timestamp=timestamp,

                   **process)
                )
        return [gpu_samples, gpu_card_process_stati, process_stati]

    def insert(self, message: dict[str, any]):
        samples = DBJsonImporter.parse(message)
        for sample in samples:
            if sample:
                self.db.insert(sample)

