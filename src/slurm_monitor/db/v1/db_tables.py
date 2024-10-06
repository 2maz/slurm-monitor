from __future__ import annotations
import logging
import sqlalchemy
import json
import re
import numpy as np
from typing import ClassVar, Any, Callable, TypeVar
import datetime as dt

from sqlalchemy import (
    DateTime,
    Float,
    ForeignKey,
    ForeignKeyConstraint,
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

T = TypeVar("T")


def Column(*args, **kwargs):
    kwargs.setdefault("nullable", False)
    return sqlalchemy.Column(*args, **kwargs)


@as_declarative()
class TableBase:
    __table__: ClassVar[Any]
    __tablename__: ClassVar[str]
    metadata: ClassVar[Any]

    _primary_key_columns: ClassVar[list[str]] = None
    _non_primary_key_columns: ClassVar[list[str]] = None

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

    @classmethod
    def primary_key_columns(cls):
        if not cls._primary_key_columns:
            cls._primary_key_columns = [x.name for x in cls.__table__.columns if x.primary_key]
        return cls._primary_key_columns

    @classmethod
    def non_primary_key_columns(cls):
        if not cls._non_primary_key_columns:
            cls._non_primary_key_columns = [x.name for x in cls.__table__.columns if not x.primary_key]
        return cls._non_primary_key_columns

    def get_timeseries_id(self) -> str:
        """
        Get the id for the timeseries - so excluding the timestamp field
        """
        return '.'.join([str(getattr(self, x)) for x in self.primary_key_columns() if x != "timestamp"])

    @classmethod
    def merge(cls,
            samples: list[T],
            merge_op: Callable[list[int | float]] | None = np.mean) -> T:
        values = {}

        reference_sample = samples[-1]
        reference_sample_timeseries_id = reference_sample.get_timeseries_id()
        for sample in samples:
            timeseries_id = sample.get_timeseries_id()
            assert timeseries_id == reference_sample_timeseries_id, \
                    f"sample id {timeseries_id} does not match reference_sample {reference_sample_timeseries_id}"

            for attribute in cls.non_primary_key_columns():
                value = getattr(sample, attribute)
                if attribute not in values:
                    values[attribute] = [value]
                else:
                    values[attribute].append(value)
        kwargs = {}

        static_columns = ["timestamp"]
        static_columns.extend(cls.primary_key_columns())

        for column_name in static_columns:
            kwargs[column_name] = getattr(reference_sample, column_name)

            for column_name in cls.non_primary_key_columns():
                try:
                    kwargs[column_name] = merge_op(values[column_name])
                except TypeError as e:
                    column = getattr(cls, column_name)
                    if column.nullable or column.type.python_type == str:
                        kwargs[column_name] = getattr(reference_sample, column_name)
                    else:
                        raise RuntimeError(f"Merging failed for column: '{column_name}'") from e

        return cls(**kwargs)

    @classmethod
    def apply_resolution(
            cls, data: list[TableBase], resolution_in_s: int,
    ) -> list[TableBase]:
        smoothed_data = []
        samples_in_window = {}

        base_time = None
        window_start_time = None
        window_index = 0

        if not data:
            return data

        if not hasattr(data[0], "timestamp"):
            raise ValueError(
                    "TableBase.apply_resolution can only be applied to "
                    "types with a 'timestamp' column"
            )

        # requiring ordered list (oldest first)
        if data[0].timestamp > data[-1].timestamp:
            data.reverse()

        for sample in data:
            timeseries_id = sample.get_timeseries_id()

            sample_timestamp = sample.timestamp
            if type(sample.timestamp) == str:
                sample_timestamp = dt.datetime.fromisoformat(sample.timestamp)

            if not base_time:
                base_time = sample_timestamp
                window_start_time = base_time
                window_index = 0

            if (
                sample_timestamp - window_start_time
            ).total_seconds() < resolution_in_s:
                if timeseries_id not in samples_in_window:
                    samples_in_window[timeseries_id] = [sample]
                else:
                    samples_in_window[timeseries_id].append(sample)
            else:
                smoothed_data.append(sample.merge(samples_in_window[timeseries_id]))
                window_index += 1

                samples_in_window[timeseries_id] = [sample]
                window_start_time = base_time + dt.timedelta(seconds=window_index*resolution_in_s)


        for _, values in samples_in_window.items():
            if values:
                smoothed_data.append(sample.merge(values))

        return smoothed_data


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
    admin_comment = Column(String(255), default="")
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
    state_description = Column(String(255))  # "",
    state_reason = Column(String(255))  # "None",
    # standard_error": "/home/.../scripts/logs/%j-stderr.txt",
    # standard_input": "/dev/null",
    # standard_output": "/home/.../scripts/logs/%j-stdout.txt",

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
        mapped_data = {}
        for k, v in data.items():
            if k in mapper.column_attrs:
                if type(v) == int and type(cls.__table__.columns[k].type) == DateTime:
                    mapped_data[k] = dt.datetime.utcfromtimestamp(v)
                else:
                    mapped_data[k] = v

        return cls(**mapped_data)

class ProcessStatus(TableBase):
    __tablename__ = "process_status"

    pid = Column(Integer, index=True, primary_key=True)
    job_id = Column(Integer, primary_key=True)
    job_submit_time = Column(DateTime, index=True, primary_key=True)
    node = Column(String(255), ForeignKey("nodes.name"), primary_key=True)

    __table_args__ = (
        ForeignKeyConstraint([job_id, job_submit_time], [JobStatus.job_id, JobStatus.submit_time]),
        {
        'timescaledb_hypertable': {
            'time_column_name': 'timestamp',
            'chunk_time_interval': '24 hours',
            }
        }
    )

    cpu_percent = Column(Float)
    memory_percent = Column(Float)

    timestamp = Column(DateTime(), default=dt.datetime.now, primary_key=True)



class Nodes(TableBase):
    __tablename__ = "nodes"

    name = Column(String(255), index=True, primary_key=True)

    cpu_count = Column(Integer)
    cpu_model = Column(String(255), nullable=True)
    memory_total = Column(BigInteger, default=0)

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

class MemoryStatus(TableBase):
    __tablename__ = "memory_status"
    __table_args__ = ({
        'timescaledb_hypertable': {
            'time_column_name': 'timestamp',
            'chunk_time_interval': '24 hours',
        }
    })

    node = Column(String(255), ForeignKey("nodes.name"), primary_key=True)

    total = Column(BigInteger)
    available = Column(BigInteger)
    percent = Column(Float)
    used = Column(BigInteger)
    free = Column(BigInteger)
    active = Column(BigInteger)
    inactive = Column(BigInteger)
    buffers = Column(BigInteger)
    cached = Column(BigInteger)
    shared = Column(BigInteger)
    slab = Column(BigInteger)

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
