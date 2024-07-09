import logging
import sqlalchemy
from contextlib import contextmanager
from typing import ClassVar, Any
import datetime as dt

from pydantic import ConfigDict, BaseModel
from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Text,
    event,
    create_engine,
    insert,
    inspect,
    types
)
from sqlalchemy.orm import DeclarativeMeta, as_declarative, sessionmaker
from sqlalchemy.engine.url import URL, make_url

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
        return ((c.key, getattr(self, c.key)) for c in inspect(self).mapper.column_attrs)

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
    node = Column(String(255), ForeignKey("nodes.name"), index=True)
    temperature_gpu = Column(Integer)
    power_draw = Column(Float)
    utilization_gpu = Column(Integer)

    utilization_memory = Column(Integer)
    memory_total = Column(Integer)

    # Only Nvidia
    pstate = Column(String(10), nullable=True)

    timestamp = Column(String(255), index=True, primary_key=True) 

