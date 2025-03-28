from __future__ import annotations
import logging
import sqlalchemy
import json
import re
import numpy as np
from typing import ClassVar, Any, Callable, TypeVar
import datetime as dt

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    ForeignKeyConstraint,
    BigInteger,
    Integer,
    inspect,
    types,
    String,
    Text,
    TIMESTAMP,
)

from sqlalchemy.orm import as_declarative, class_mapper
# https://pydoc.dev/sqlalchemy/latest/sqlalchemy.dialects.postgresql.ARRAY.html
# https://docs.sqlalchemy.org/en/14/dialects/postgresql.html#sqlalchemy.dialects.postgresql.hstore
from sqlalchemy.dialects.postgresql import (
    HSTORE,
    ARRAY
)

from sqlalchemy.sql.functions import GenericFunction
from sqlalchemy.ext.compiler import compiles


logger = logging.getLogger(__name__)

T = TypeVar("T")


# Ensure consistent time handling
class EpochFn(GenericFunction):
    type = DateTime()
    inherit_cache = True

# For PostgreSQL, we will use the `EXTRACT(EPOCH FROM <datetime>)` syntax
@compiles(EpochFn, 'postgresql')
def compile_postgresql(expr, compiler, **kwargs):
    return f"EXTRACT(EPOCH FROM {compiler.process(expr.clauses.clauses[0], **kwargs)})"

# For TimeScaledb
@compiles(EpochFn, 'timescaledb')
def compile_timescaledb(expr, compiler, **kwargs):
    return f"EXTRACT(EPOCH FROM {compiler.process(expr.clauses.clauses[0], **kwargs)})"

# For SQLite, we use `strftime('%s', datetime_column)` to get epoch
@compiles(EpochFn, 'sqlite')
def compile_sqlite(expr, compiler, **kwargs):
    return f"strftime('%s', {compiler.process(expr.clauses.clauses[0], **kwargs)})"

class time_bucket(GenericFunction):
    type = TIMESTAMP()
    inherit_cache = True

# For TimeScaledb
@compiles(time_bucket, 'timescaledb')
def compile_timescaledb(expr, compiler, **kwargs):
    time_window = expr.clauses.clauses[0].value
    time_column = f"{compiler.process(expr.clauses.clauses[1], **kwargs)}"

    if type(time_window) == int:
        return f"time_bucket('{time_window} seconds',{time_column})"
    else:
        return f"time_bucket('{time_window}',{time_column}"


def Column(*args, **kwargs):
    if "nullable" not in kwargs:
        kwargs.setdefault("nullable", False)

    column_type = args[0]
    if 'default' not in kwargs:
        if column_type in [Integer, BigInteger, Float]:
            kwargs.setdefault('default', 0)
        elif column_type in [Text, String]:
            kwargs.setdefault('default', '')

    comment = {}
    if "desc" in kwargs:
        comment["desc"] = kwargs['desc']
        del kwargs['desc']

    if "unit" in kwargs:
        comment["unit"] = kwargs['unit']
        del kwargs['unit']

    if comment:
        kwargs["comment"] = json.dumps(comment)

    return sqlalchemy.Column(*args, **kwargs)


class HStoreModel(types.TypeDecorator):
    impl = HSTORE

    required: ClassVar[list[str]] = []
    optional: ClassVar[list[str]] = []

    @property
    def allowed(self):
        return self.required + self.optional

    def process_bind_param(self, value, dialect):
        if value is None or type(value) != dict:
            raise KeyError(f"{self.__class__}: value must be dictionary")

        for key in self.required:
            if key not in value:
                raise KeyError(f"{self.__class__}: value misses the required key '{key}'")

        for key in value:
            if key not in self.allowed:
                raise KeyError(f"{self.__class__}: value contains an invalid key '{key}'."
                    " Permitted are {','.join(allowed)}")
        return value

    def process_result_value(self, value, dialect):
        return value




# core-model: (index: unsigned, physical: unsigned, model: string)
#class CoreModel(HStoreModel):
#    required: ClassVar[list[str]] = ["index", "physical", "model"]
#
#class SoftwareVersion(HStoreModel):
#    required: ClassVar[list[str]] = ["key", "version"]
#    optional: ClassVar[list[str]] = ["name"]

#@dataclasses.dataclass
#class CoreModel:
#    index: int
#    physical: int
#    model: str
#

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
        if dialect.name in ["default", "sqlite", "postgresql", "timescaledb"]:
            return self.impl

        # TODO: if requiring postgresql check the JSON type:
        # https://docs.sqlalchemy.org/en/20/dialects/postgresql.html#sqlalchemy.dialects.postgresql.JSON
        raise NotImplementedError(
            f"The field type for the encountered database dialect '{dialect.name}' has not been "
            f"specified - please inform the developer to add support"
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

class UUID(types.TypeDecorator):
    impl = String
    cache_ok = True


class Node(TableBase):
    __tablename__ = "node"
    __table_args__ = (
        {}
    )
    cluster = Column(String, primary_key=True)
    node = Column(String, primary_key=True)

    architecture = Column(String)

class NodeConfig(TableBase):
    __tablename__ = "node_config"

    cluster = Column(Text, primary_key=True)
    node = Column(Text, primary_key=True)

    os_name = Column(Text)
    os_release = Column(Text)
    sockets = Column(Integer)
    cores_per_socket = Column(Integer)
    threads_per_core = Column(Integer)
    cpu_model = Column(String)
    architecture = Column(String)

    description = Column(Text)

    memory = Column(BigInteger, desc="primary memory", unit="kilobyte")
    topo_svg = Column(Text, default=None, nullable=True)

    # TBD: array of gpu-card values # considering here uuids
    cards = Column(ARRAY(UUID), desc="Array of gpu-card uuid", default=[])

    timestamp = Column(DateTime(timezone=True), default=dt.datetime.now, primary_key=True)

    __table_args__ = (
        ForeignKeyConstraint([cluster, node], [Node.cluster, Node.node]),
        {}
    )

class SoftwareVersion(TableBase):
    __tablename__ = "software_version"
    cluster = Column(Text, primary_key=True)
    node = Column(Text, primary_key=True)

    key = Column(String, primary_key=True)
    name = Column(String)
    version = Column(String, primary_key=True)

class GPUCard(TableBase):
    __tablename__ = "gpu_card"
    __table_args__ = (
        *[CheckConstraint(f"{x} >= 0", name=f"{x}_is_not_negative") for x in [
            'memory',
        ]],
        {}
    )
    uuid = Column(UUID, index=True, primary_key=True)

    manufacturer = Column(Text)
    model = Column(Text)
    architecture = Column(Text)
    memory = Column(Integer)

class GPUCardConfig(TableBase):
    """
    Collect dynamic properties of the GPU in this table
    """
    __tablename__ = "gpu_card_config"

    uuid = Column(UUID, ForeignKey("gpu_card.uuid"), primary_key=True)

    address = Column(Text)
    power_limit = Column(Integer)
    max_power_limit = Column(Integer)
    min_power_limit = Column(Integer)
    max_ce_clock = Column(Integer)
    max_memory_clock = Column(Integer)

    driver = Column(String)
    firmware = Column(String)

    # node name the card is attached to
    cluster = Column(String)
    node = Column(String)

    # Card local index
    index = Column(Integer, primary_key=True)

    # Validity - since the card might not be present for some intervals
    # TDB: do we need to consider this
    #start_time = Column(DateTime(timezone=True), primary_key=True, default=dt.datetime(2025,1,1))
    #end_time = Column(DateTime(timezone=True), default=dt.datetime(2100,12,31))

    timestamp = Column(DateTime(timezone=True), primary_key=True)

    __table_args__ = (
        ForeignKeyConstraint([cluster, node], [Node.cluster, Node.node]),
        *[CheckConstraint(f"{x} >= 0", name=f"{x}_is_not_negative") for x in [
            'power_limit',
            'max_power_limit',
            'min_power_limit',
            'max_ce_clock',
            'max_memory_clock',
        ]],
        {}
    )


class GPUCardStatus(TableBase):
    __tablename__ = "gpu_card_status"
    __table_args__ = (
        *[CheckConstraint(f"{x} >= 0", name=f'{x}_is_not_negative') for x in [
            'index',
            'fan',
            'failing',
            'memory',
            'ce_util',
            'ce_clock',
            'memory_util',
            'memory_clock',
            'power',
            'power_limit',
            ]
        ],
        {
            'timescaledb_hypertable': {
                'time_column_name': 'timestamp',
                'chunk_time_interval': '24 hours',
                'compression': {
                    'segmentby': 'uuid, index',
                    'orderby': 'timestamp',
                    'interval': '7 days'
                }
        }
    })


    # local card index, may change at boot
    index = Column(Integer, nullable=True)

    # Card UUI
    uuid = Column(UUID, ForeignKey("gpu_card.uuid"), index=True, primary_key=True)

    # Indicate a failure condition, true meaning failure
    failing = Column(Integer)

    # percent of primary fans' max speed - max exceed 100% on some cards
    fan = Column(Integer)

    # current compute mode: card-specific if known at all
    compute_mode = Column(Text)

    # current performance level, card-specific >= 0, 0 for 'unknown'
    performance_state = Column(Integer)

    # kB of memory_use
    memory = Column(Integer)

    # percent of computing element capability used
    ce_util = Column(Integer)

    # percent of memory used
    memory_util = Column(Integer)

    # degree C card temperature at primary sensor
    temperature = Column(Integer)

    # current power usage in W
    power = Column(Integer)

    # power limit in W
    power_limit = Column(Integer)

    ce_clock = Column(Integer)
    memory_clock = Column(Integer)

    timestamp = Column(DateTime(timezone=True), default=dt.datetime.now, primary_key=True)

class GPUCardProcessStatus(TableBase):
    __tablename__ = "gpu_card_process_status"
    __table_args__ = (
        {
            'timescaledb_hypertable': {
                'time_column_name': 'timestamp',
                'chunk_time_interval': '24 hours',
                'compression': {
                    'segmentby': 'uuid, index, pid',
                    'orderby': 'timestamp',
                    'interval': '7 days'
                }
            }

        }
    )

    pid = Column(BigInteger, primary_key=True)

    job = Column(BigInteger, primary_key=True)
    epoch = Column(BigInteger,
            desc="Bootcycle presentation of node - continuously increasing number",
            primary_key=True)
    user = Column(String)

    # TBD: Could consider 'node' as redundant entry in here
    # node = Column(Text)

    # Card UUID
    uuid = Column(UUID, ForeignKey('gpu_card.uuid'), index=True, primary_key=True)
    index = Column(Integer)

    gpu_util = Column(Float)
    # in kilobytes
    gpu_memory = Column(Float)
    gpu_memory_util = Column(Float)

    timestamp = Column(DateTime(timezone=True), default=dt.datetime.now, primary_key=True)

class ProcessStatus(TableBase):
    __tablename__ = "process_status"

    cluster = Column(String, primary_key=True)
    node = Column(String, primary_key=True)

    job = Column(BigInteger, primary_key=True)
    # Slurm jobs will have epoch = 0
    epoch = Column(BigInteger,
            desc="Bootcycle presentation of node - continuously increasing number",
            primary_key=True)

    user = Column(String)

    # rest of process sample
    pid = Column(BigInteger, primary_key=True)
    ppid = Column(BigInteger)

    # private resident memory
    resident = Column(BigInteger, unit='kilobyte')

    # virtual data+stack memory
    virtual = Column(BigInteger, unit='kilobyte')

    # command (not the command line) - '_unknown_' for zombie processes
    cmd = Column(Text)

    cpu_avg = Column(Float,
            desc="""
            The running average CPU percentage over the true lifetime of the process
            as reported by the operating system. 100.0 corresponds to 'one full core's
            worth of computation'
            """,
            unit='percent'
            )
    cpu_util = Column(Float,
            desc="""
            The current  cpu utilization of the process, 100.0 corresponds
            to 'one full core's worth of computation'
            """,
            unit='percent'
            )
    cpu_time = Column(BigInteger,
            desc="""
            The number of additional process with the same job and cmd and
            no child processes that have been rolled into this one. That is,
            if the value is 1, the record represents the sum of the data for two processes
            """,
            unit='seconds')

    timestamp = Column(DateTime(timezone=True), default=dt.datetime.now, primary_key=True)
    # Consider
    # rolled_up =

    __table_args__ = (
        ForeignKeyConstraint([cluster, node], [Node.cluster, Node.node]),
        CheckConstraint("job != 0 or epoch != 0", "job_or_epoch_non_zero"),
        CheckConstraint("job >= 0 and epoch >= 0", "job_or_epoch_non_negative"),
        CheckConstraint("pid >= 0 and ppid >= 0", "pid_and_ppid_non_negative"),
        {
            'timescaledb_hypertable': {
                'time_column_name': 'timestamp',
                'chunk_time_interval': '24 hours',
                'compression': {
                    'segmentby': 'job, epoch',
                    'orderby': 'timestamp',
                    'interval': '7 days'
                }
            }

        }
    )



#class JobStatus(TableBase):
#    __tablename__ = "job_status"
#    __table_args__ = (
#        CheckConstraint("job != 0 or epoch != 0", "job_or_epoch_non_zero"),
#        CheckConstraint("job >= 0 and epoch >= 0", "job_or_epoch_non_negative"),
#        {
#            'timescaledb_hypertable': {
#                'time_column_name': 'timestamp',
#                'chunk_time_interval': '24 hours',
#                'compression': {
#                    'segmentby': 'job, epoch',
#                    'orderby': 'timestamp',
#                    'interval': '7 days'
#                }
#            }
#
#        }
#    )
#    job = Column(BigInteger)


class TableMetadata(TableBase):
    __tablename__ = "metadata"

    table = Column(Text, primary_key=True)
    field = Column(Text, primary_key=True)
    description = Column(Text)
    unit = Column(Text)


class SlurmJobStatus(TableBase):
    __tablename__ = "slurm_job_status"
    __table_args__ = (
        CheckConstraint("job_id != 0 or timestamp != 0", "job_or_timestamp_non_zero"),
        CheckConstraint("job_id >= 0 and timestamp >= 0", "job_or_timestamp_non_negative"),
        {
            'timescaledb_hypertable': {
                'time_column_name': 'timestamp',
                'chunk_time_interval': '24 hours',
                'compression': {
                    'segmentby': 'cluster, job',
                    'orderby': 'timestamp',
                    'interval': '7 days'
                }
            }

        }
    )

    # JobID
    # The number of the job or job step. It is in the form: job.jobstep.
    # Meanwhile here - we
    job_id = Column(BigInteger, index=True, primary_key=True)  # ": 244843,
    job_step = Column(Integer)

    # JobIDRaw
    # In case of job array print the JobId instead of the ArrayJobId. For non
    # job arrays the output is the JobId in the format job.jobstep.
    job_id_raw = Column


    submit_time = Column(DateTime, index=True, primary_key=True)  #

    name = Column(Text)
    start_time = Column(DateTime, nullable=True)
    end_time = Column(DateTime, nullable=True)

    account = Column(Text)
    accrue_time = Column(BigInteger)
    admin_comment = Column(Text)
    array_job_id = Column(Integer, nullable=True)  # 244843
    array_task_id = Column(Integer, nullable=True)  # 984
    array_max_tasks = Column(Integer)  # 20
    array_task_string = Column(Text)  #
    association_id = Column(Integer)  # ": 0,
    # batch_features": "",
    # batch_flag": true,
    batch_host = Column(Text)
    # flags": [],
    # burst_buffer": "",
    # burst_buffer_state": "",

    cluster = Column(Text)

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
    eligible_time = Column(Integer, nullable=True)  # ": 1720736375,

    # excluded_nodes": "",
    exit_code = Column(BigInteger)  # ": 0,
    # features": "",
    # federation_origin": "",
    # federation_siblings_active": "",
    # federation_siblings_viable": "",
    gres_detail = Column(GPUIdList, default=[])
    group_id = Column(Integer, nullable=True)  # ": 5000,
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
    job_state = Column(Text)  # ": "COMPLETED",
    # last_sched_evaluation": 1720736375,
    # licenses": "",
    # max_cpus": 0,
    # max_nodes": 0,
    # mcs_label": "",
    # memory_per_tres": "",
    # name": "seidr",
    nodes = Column(Text)  # "n042",
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
    partition = Column(Text)  # "slowq",
    memory_per_node = Column(Integer)
    memory_per_cpu = Column(Integer)
    minimum_cpus_per_node = Column(Integer)
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
    state_description = Column(Text)  # "",
    state_reason = Column(Text)  # "None",
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
    user_name = Column(Text, nullable=True)
    # wckey": "",
    # current_working_directory": "/global/D1/homes/..."
    # id = Column(Integer) # 244843

    timestamp = Column(DateTime(timezone=True), default=dt.datetime.now, primary_key=True)

    @classmethod
    def from_json(cls, data) -> SlurmJobStatus:
        mapper = class_mapper(cls)
        mapped_data = {}
        for k, v in data.items():
            if k in mapper.column_attrs:
                if type(v) == int and type(cls.__table__.columns[k].type) == DateTime:
                    mapped_data[k] = dt.datetime.fromtimestamp(v, dt.timezone.utc)
                else:
                    mapped_data[k] = v

        return cls(**mapped_data)
