from __future__ import annotations
from slurm_monitor.db.v2.db_tables import ErrorMessage
import dataclasses
from enum import Enum

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
    data: Data | None
    errors: list[ErrorMessage] | None


class TopicType(str, Enum):
    cluster = 'cluster'
    job = 'job'
    sample = 'sample'
    sysinfo = 'sysinfo'

    def get_topic(self, cluster: str) -> str:
        return f"{cluster}.{self.value}"

    @classmethod
    def infer(cls, topic: str) -> TopicType:
        for t in cls:
            if topic.endswith(f".{t.value}"):
                return t
        raise ValueError("TopicType.infer: '{topic}' does not have a value topic type suffix")
