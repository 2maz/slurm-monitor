from __future__ import annotations
import logging
import sqlalchemy
import json
import re
import numpy as np
from typing import ClassVar, Any, Callable
import datetime as dt

from sqlalchemy import (
    DateTime,
    Float,
    ForeignKey,
    BigInteger,
    Integer,
    String,
    inspect,
    types,
    Text,
)
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.orm import as_declarative, class_mapper

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

    def __iter__(self):
        return (
            (c.key, getattr(self, c.key)) for c in inspect(self).mapper.column_attrs
        )

    def _asdict(self):
        return dict(self)

    def __eq__(self, other):
        return type(self) == type(other) and tuple(self) == tuple(other)

# Define a custom column type to process logical ids from text using
# transform
class GPUIdList(types.TypeDecorator):
    impl = Text
    cache_ok = True

    def load_dialect_impl(self, dialect):
        """
        see https://docs.sqlalchemy.org/en/20/core/custom_types.html#sqlalchemy.types.TypeDecorator.load_dialect_impl
        """
        if dialect.name == "sqlite" or dialect.name == "timescaledb":
            return self.impl

        if dialect.name == "mysql":
            return dialect.type_descriptor(LONGTEXT)

        # TODO: if requiring postgresql check the JSON type:
        # https://docs.sqlalchemy.org/en/20/dialects/postgresql.html#sqlalchemy.dialects.postgresql.JSON
        raise NotImplementedError(
            "The field type for the encountered database dialect '{dialect.name}' has not been"
            "specified - please inform the developer to add support"
        )

    @classmethod
    def get_logical_ids(cls, value: int | str):
        if type(value) == int:
            return value

        m = re.match(r".*\(IDX:(.*)\)", value)
        indices = m.group(1)
        idx_ranges = indices.split(",")
        gpu_logical_ids = []

        for idx_range in idx_ranges:
            if "-" in idx_range:
                m_range = re.match(r"([0-9]+)-([0-9]+)", idx_range)
                start_idx = int(m_range.group(1))
                end_idx = int(m_range.group(2))
                gpu_logical_ids.extend(range(start_idx, end_idx + 1))
            else:
                gpu_logical_ids.append(int(idx_range))
        return gpu_logical_ids

    def transform_input(self, value: list[str|int]):
        if len(set(value)) > 1:
            if type(value[0]) == str:
                raise RuntimeError(f"Assuming maximum length of 1 for GPU details, but found: {value}")
            elif type(value[0]) == int:
                return value
            else:
                raise RuntimeError(f"Wrong list type in {value}, expected str|int")

        for x in value:
            return self.get_logical_ids(x)
        return []

    def transform_output(self, value):
        return value

    def process_bind_param(self, value, dialect):
        if value is not None:
            return json.dumps(self.transform_input(value))

    def process_result_value(self, value, dialect):
        if value is None:
            return

        try:
            return self.transform_output(json.loads(value))
        except json.JSONDecodeError as e:
            logger.error(f"Processing result value failed: {value} -- {e}")
            raise

class GPUs(TableBase):
    __tablename__ = "gpus"

    uuid = Column(String(64), index=True, primary_key=True)
    node = Column(String(255), ForeignKey("nodes.name"))

    model = Column(String(255))
    local_id = Column(Integer)
    memory_total = Column(BigInteger)

class JobStatus(TableBase):
    __tablename__ = "job_status"

    job_id = Column(Integer, index=True, primary_key=True)  # ": 244843,
    submit_time = Column(DateTime, index=True, primary_key=True)  #

    name = Column(String(255))
    start_time = Column(DateTime, nullable=True)
    end_time = Column(DateTime, nullable=True)

    account = Column(String(100))
    accrue_time = Column(BigInteger)
    admin_comment = Column(String(255))
    array_job_id = Column(Integer)  # 244843
    array_task_id = Column(Integer, nullable=True)  # 984
    array_max_tasks = Column(Integer)  # 20
    array_task_string = Column(String(255))  #
    association_id = Column(Integer)  # ": 0,
    # batch_features": "",
    # batch_flag": true,
    batch_host = Column(String(50))
    # flags": [],
    # burst_buffer": "",
    # burst_buffer_state": "",
    cluster = Column(String(32))  # "slurm"
    # cluster_features": "",
    # command": "/global/D1/homes/.."
    # comment": "",
    # contiguous": false,
    # core_spec": null,
    # thread_spec": null,
    # cores_per_socket": null,
    # billable_tres": 1,
    # cpus_per_task": null,
    # cpu_frequency_minimum": null,
    # cpu_frequency_maximum": null,
    # cpu_frequency_governor": null,
    # cpus_per_tres": "",
    # deadline": 0,
    # delay_boot": 0,
    # dependency": "",
    derived_exit_code = Column(BigInteger)  # ": 256,
    eligible_time = Column(Integer)  # ": 1720736375,
    end_time = Column(Integer)  # ": 1720739331,
    # excluded_nodes": "",
    exit_code = Column(BigInteger)  # ": 0,
    # features": "",
    # federation_origin": "",
    # federation_siblings_active": "",
    # federation_siblings_viable": "",
    gres_detail = Column(GPUIdList)
    group_id = Column(Integer)  # ": 5000,
    # job_resources": {
    # "nodes": "n042",
    # "allocated_cpus": 1,
    # "allocated_hosts": 1,
    # "allocated_nodes": {
    #   "0": {
    #     "sockets": {
    #       "1": "unassigned"
    #     },
    #     "cores": {
    #       "0": "unassigned"
    #     },
    #     "memory": 0,
    #     "cpus": 1
    #   }
    # }
    # ,
    job_state = Column(String(25))  # ": "COMPLETED",
    # last_sched_evaluation": 1720736375,
    # licenses": "",
    # max_cpus": 0,
    # max_nodes": 0,
    # mcs_label": "",
    # memory_per_tres": "",
    # name": "seidr",
    nodes = Column(String(128))  # "n042",
    # nice": null,
    # tasks_per_core": null,
    # tasks_per_node": 0,
    # tasks_per_socket": null,
    # tasks_per_board": 0,
    cpus = Column(Integer)  # 1
    node_count = Column(Integer)  # 1
    tasks = Column(Integer)  # 1,
    # het_job_id": 0,
    # het_job_id_set": "",
    # het_job_offset": 0,
    partition = Column(String(255))  # "slowq",
    # memory_per_node": null,
    # memory_per_cpu": null,
    # minimum_cpus_per_node": 1,
    # minimum_tmp_disk_per_node": 0,
    # preempt_time": 0,
    # pre_sus_time": 0,
    # priority": 4294726264,
    # profile": null,
    # qos": "normal",
    # reboot": false,
    # required_nodes": "",
    # requeue": true,
    # resize_time": 0,
    # restart_cnt": 0,
    # resv_name": "",
    # shared": null,
    # show_flags": [
    # "SHOW_ALL",
    # "SHOW_DETAIL",
    # "SHOW_LOCAL"
    # ,
    # sockets_per_board": 0,
    # sockets_per_node": null,
    start_time = Column(Integer)  # 1720736375,
    state_description = Column(String(255))  # "",
    state_reason = Column(String(255))  # "None",
    # standard_error": "/home/.../scripts/logs/%j-stderr.txt",
    # standard_input": "/dev/null",
    # standard_output": "/home/.../scripts/logs/%j-stdout.txt",
    submit_time = Column(Integer)  # 1720627330,
    suspend_time = Column(Integer)  # 0,
    # system_comment": "",
    time_limit = Column(Integer, nullable=True)  # 7200,
    # time_minimum": 0,
    # threads_per_core": null,
    # tres_bind": "",
    # tres_freq": "",
    # tres_per_job": "",
    # tres_per_node": "",
    # tres_per_socket": "",
    # tres_per_task": "",
    # tres_req_str": "cpu=1,node=1,billing=1",
    # tres_alloc_str": "cpu=1,billing=1",
    user_id = Column(Integer)  # 6500,
    # user_name": "testuser",
    # wckey": "",
    # current_working_directory": "/global/D1/homes/..."
    # id = Column(Integer) # 244843

    @classmethod
    def from_json(cls, data) -> JobStatus:
        mapper = class_mapper(cls)
        mapped_data = {k: v for k, v in data.items() if k in mapper.attrs}
        return cls(**mapped_data)


class Nodes(TableBase):
    __tablename__ = "nodes"

    name = Column(String(255), index=True, primary_key=True)

    cpu_count = Column(Integer)
    cpu_model = Column(String(255), nullable=True)

class CPUStatus(TableBase):
    __tablename__ = "cpu_status"
    __table_args__ = ({
        'timescaledb_hypertable': {
            'time_column_name': 'timestamp',
            'chunk_time_interval': '24 hours',
        }
    })

    node = Column(String(255), ForeignKey("nodes.name"), primary_key=True)
    local_id = Column(Integer, primary_key=True)
    cpu_percent = Column(Float)

    timestamp = Column(DateTime(), default=dt.datetime.now, primary_key=True)

class GPUStatus(TableBase):
    __tablename__ = "gpu_status"
    __table_args__ = ({
        'timescaledb_hypertable': {
            'time_column_name': 'timestamp',
            'chunk_time_interval': '24 hours',
        }
    })
    uuid = Column(String(64), ForeignKey("gpus.uuid"), index=True, primary_key=True)

    temperature_gpu = Column(Float)
    power_draw = Column(Float)
    utilization_gpu = Column(Float)

    utilization_memory = Column(Float)

    # Only Nvidia
    pstate = Column(String(10), nullable=True)

    timestamp = Column(DateTime(), default=dt.datetime.now, primary_key=True)

    @classmethod
    def merge(cls, samples: list[GPUStatus], merge_op: Callable[list[int | float]] | None = np.mean) -> GPUStatus:
        values = {}

        reference_sample = samples[-1]
        for sample in samples:
            assert sample.uuid == reference_sample.uuid, \
                    f"sample uuid {sample.uuid} does not match reference_sample {reference_sample.uuid}"
            for attribute in ["temperature_gpu", "power_draw", "utilization_gpu", "utilization_memory"]:
                value = getattr(sample, attribute)
                if attribute not in values:
                    values[attribute] = [value]
                else:
                    values[attribute].append(value)

        return cls(
            uuid=reference_sample.uuid,
            temperature_gpu=merge_op(values["temperature_gpu"]),
            power_draw=merge_op(values["power_draw"]),
            utilization_gpu=merge_op(values["utilization_gpu"]),
            utilization_memory=merge_op(values["utilization_gpu"]),
            timestamp=reference_sample.timestamp,
        )
