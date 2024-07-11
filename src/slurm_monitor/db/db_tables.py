from __future__ import annotations
import logging
import sqlalchemy
from typing import ClassVar, Any

from sqlalchemy import DateTime, Float, ForeignKey, Integer, String, inspect
from sqlalchemy.orm import as_declarative

logger = logging.getLogger(__name__)


def Column(*args, **kwargs):
    kwargs.setdefault("nullable", False)
    return sqlalchemy.Column(*args, **kwargs)


@as_declarative()
class TableBase:
    __table__: ClassVar[Any]
    __tablename__: ClassVar[str]
    metadata: ClassVar[Any]

    def __init__(self, **kwargs):
        pass

    def __iter(self):
        return (
            (c.key, getattr(self, c.key)) for c in inspect(self).mapper.column_attrs
        )

    def _asdict(self):
        return dict(self)

    def __eq__(self, other):
        return type(self) == type(other) and tuple(self) == tuple(other)


class Nodes(TableBase):
    __tablename__ = "nodes"
    name = Column(String(255), index=True, primary_key=True)


class Jobs(TableBase):
    __tablename__ = "jobs"

    name = Column(String(255), index=True, primary_key=True)
    submit_time = Column(DateTime, nullable=False)
    start_time = Column(DateTime, nullable=True)
    end_time = Column(DateTime, nullable=True)


class GPUStatus(TableBase):
    __tablename__ = "gpu_status"

    name = Column(String(255), index=True, primary_key=True)
    uuid = Column(String(64), index=True, primary_key=True)
    local_id = Column(Integer)
    node = Column(String(255), ForeignKey("nodes.name"), index=True)
    temperature_gpu = Column(Integer)
    power_draw = Column(Float)
    utilization_gpu = Column(Integer)

    utilization_memory = Column(Integer)
    memory_total = Column(Integer)

    # Only Nvidia
    pstate = Column(String(10), nullable=True)

    timestamp = Column(String(255), index=True, primary_key=True)

    def merge(self, other, merge_op=max) -> GPUStatus:
        assert self.uuid == other.uuid
        return GPUStatus(
            name=self.name,
            uuid=self.uuid,
            node=self.node,
            temperature_gpu=merge_op(self.temperature_gpu, other.temperature_gpu),
            power_draw=merge_op(self.power_draw, other.power_draw),
            utilization_gpu=merge_op(self.utilization_gpu, other.utilization_gpu),
            utilization_memory=merge_op(
                self.utilization_memory, other.utilization_memory
            ),
            memory_total=merge_op(self.memory_total, other.memory_total),
            timestamp=other.timestamp,
        )
