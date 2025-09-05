from __future__ import annotations
import logging
import sqlalchemy
import json
import re
import numpy as np
from typing import ClassVar, Any, Callable, TypeVar
import datetime as dt

import enum
from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    ForeignKeyConstraint,
    BigInteger,
    Index,
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
import slurm_monitor.timescaledb as timescaledb

__all__ = [ "timescaledb" ]


logger = logging.getLogger(__name__)

T = TypeVar("T")
Xint = BigInteger
DateTimeTZAware = DateTime(timezone=True)


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


def ensure_non_negative(*column_names) -> list[CheckConstraint]:
    return [
       CheckConstraint(f"{x} >= 0", name=f"{x}_is_not_negative") for x in column_names
    ]


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
        return '.'.join([str(getattr(self, x)) for x in self.primary_key_columns() if x != "time"])

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

        static_columns = ["time"]
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

class Cluster(TableBase):
    __tablename__ = "cluster_attributes"
    __table_args__ = (
        {
            'info': { 'sonar_spec': 'ClusterAttributes' },
            'timescaledb_hypertable': {
                'time_column_name': 'time',
                'chunk_time_interval': '24 hours',
                'compression': {
                    'segmentby': 'cluster',
                    'orderby': 'time',
                    'interval': '7 days'
                }
            }
        }
    )
    cluster = Column(String, primary_key=True, index=True)
    slurm = Column(Boolean, default=True)
    partitions = Column(ARRAY(String))
    nodes = Column(ARRAY(String))

    time = Column(DateTimeTZAware, default=dt.datetime.now, primary_key=True)

class Node(TableBase):
    __tablename__ = "node"

    cluster = Column(String, primary_key=True)
    node = Column(String, primary_key=True)

    architecture = Column(String)

    __table_args__ = (
            { 'info': { 'comment': 'Auxiliary class to permit foreign key constraints on cluster/node' } }
    )

class NodeState(TableBase):
    __tablename__ = "node_state"

    cluster = Column(String, primary_key=True)
    node = Column(String, primary_key=True)
    states = Column(ARRAY(String))

    time = Column(DateTimeTZAware, default=dt.datetime.now, primary_key=True)

    __table_args__ = (
        ForeignKeyConstraint([cluster, node], [Node.cluster, Node.node]),
        {
            'info': { 'sonar_spec' : "ClusterAttributes.nodes" },
            'timescaledb_hypertable': {
                'time_column_name': 'time',
                'chunk_time_interval': '24 hours',
                'compression': {
                    'segmentby': 'cluster, node',
                    'orderby': 'time',
                    'interval': '7 days'
                }
            }
        }
    )

class Partition(TableBase):
    __tablename__ = "partition"
    __table_args__ = (
        {
            'info' : {
                      'sonar_spec' : "ClusterAttributes.partitions"
                     }
            ,
            'timescaledb_hypertable': {
                'time_column_name': 'time',
                'chunk_time_interval': '24 hours',
                'compression': {
                    'segmentby': 'cluster',
                    'orderby': 'time',
                    'interval': '7 days'
                }
            }
        }
    )
    cluster = Column(String, primary_key=True, index=True)
    partition = Column(String, primary_key=True, index=True)
    nodes = Column(ARRAY(String))
    nodes_compact = Column(ARRAY(String))
    time = Column(DateTimeTZAware, default=dt.datetime.now, primary_key=True)

class SysinfoAttributes(TableBase):
    __tablename__ = "sysinfo_attributes"

    time = Column(DateTimeTZAware, default=dt.datetime.now, primary_key=True)
    cluster = Column(String, primary_key=True)
    node = Column(String, primary_key=True)

    os_name = Column(String)
    os_release = Column(String)
    architecture = Column(String)

    sockets = Column(BigInteger)
    cores_per_socket = Column(BigInteger)
    threads_per_core = Column(BigInteger)
    cpu_model = Column(String)

    memory = Column(BigInteger, desc="primary memory", unit="kilobyte")
    topo_svg = Column(Text, default=None, nullable=True)

    cards = Column(ARRAY(UUID), desc="Array of gpu-card uuid", default=[])

    # FIXME:
    # software SysinfoSoftwareVersion

    __table_args__ = (
        *ensure_non_negative("sockets", "cores_per_socket", "threads_per_core", "memory"),
        ForeignKeyConstraint([cluster, node], [Node.cluster, Node.node]),
        {
            'info': { 'sonar_spec': 'SysinfoAttributes' },
            'timescaledb_hypertable': {
                'time_column_name': 'time',
                'chunk_time_interval': '24 hours',
                'compression': {
                    'segmentby': 'cluster, node',
                    'orderby': 'time',
                    'interval': '7 days'
                }
            }
        }
    )

class SysinfoSoftwareVersion(TableBase):
    __tablename__ = "sysinfo_software_version"
    cluster = Column(String, primary_key=True)
    node = Column(String, primary_key=True)

    key = Column(String, primary_key=True)
    name = Column(String)
    version = Column(String, primary_key=True)

    __table_args__ = (
        ForeignKeyConstraint([cluster, node], [Node.cluster, Node.node]),
        {
            'info': { 'sonar_spec': 'SysinfoSoftwareVersion' }
        }
    )

class SysinfoGpuCard(TableBase):
    """
    Collect static properties of the GPU in this table
    """
    __tablename__ = "sysinfo_gpu_card"
    __table_args__ = (
        *ensure_non_negative('memory'),
        { 'info': { 'sonar_spec': 'SysinfoGpuCard', 'requires_tables': ['sysinfo_gpu_card_config'] } }
    )

    uuid = Column(UUID, primary_key=True)

    manufacturer = Column(String)
    model = Column(String)
    architecture = Column(String)
    memory = Column(BigInteger)

class SysinfoGpuCardConfig(TableBase):
    """
    Collect dynamic properties of the GPU in this table
    """
    __tablename__ = "sysinfo_gpu_card_config"

    # node name the card is attached to
    cluster = Column(String)
    node = Column(String)

    uuid = Column(UUID, ForeignKey('sysinfo_gpu_card.uuid'), primary_key=True)

    # Card local index
    index = Column(Integer, index=True)
    address = Column(String)

    driver = Column(String)
    firmware = Column(String)

    power_limit = Column(BigInteger)
    max_power_limit = Column(BigInteger)
    min_power_limit = Column(BigInteger)
    max_ce_clock = Column(BigInteger)
    max_memory_clock = Column(BigInteger)

    # Validity - since the card might not be present for some intervals
    # TDB: do we need to consider this
    #start_time = Column(DateTime(timezone=True), primary_key=True, default=dt.datetime(2025,1,1))
    #end_time = Column(DateTime(timezone=True), default=dt.datetime(2100,12,31))

    time = Column(DateTimeTZAware, primary_key=True)

    __table_args__ = (
        *ensure_non_negative(
            'index',
            'power_limit',
            'max_power_limit',
            'min_power_limit',
            'max_ce_clock',
            'max_memory_clock',
        ),
        ForeignKeyConstraint([cluster, node], [Node.cluster, Node.node]),
        {
            'info': { 'sonar_spec': 'SysinfoGpuCard', 'requires_tables': ['sysinfo_gpu_card'] },
            'timescaledb_hypertable': {
                'time_column_name': 'time',
                'chunk_time_interval': '24 hours',
                'compression': {
                    'segmentby': 'cluster, node, uuid',
                    'orderby': 'time',
                    'interval': '7 days'
                }
            }
        },
    )


class SampleGpu(TableBase):
    __tablename__ = "sample_gpu"
    __table_args__ = (
        *ensure_non_negative(
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
        ),
        {
            'info': { 'sonar_spec': 'SampleSystem.gpus' },
            'timescaledb_hypertable': {
                'time_column_name': 'time',
                'chunk_time_interval': '24 hours',
                'compression': {
                    'segmentby': 'uuid, index',
                    'orderby': 'time',
                    'interval': '7 days'
                }
            }
        }
    )


    # local card index, may change at boot
    index = Column(Integer, nullable=True)

    # Card UUI
    uuid = Column(UUID, ForeignKey("sysinfo_gpu_card.uuid"), index=True, primary_key=True)

    # Indicate a failure condition, true meaning failure
    failing = Column(BigInteger)

    # percent of primary fans' max speed - max exceed 100% on some cards
    fan = Column(BigInteger)

    # current compute mode: card-specific if known at all
    compute_mode = Column(String)

    # current performance level, card-specific >= 0, 0 for 'unknown'
    performance_state = Column(Xint)

    # kB of memory_use
    memory = Column(BigInteger)

    # percent of computing element capability used
    ce_util = Column(BigInteger)

    # percent of memory used
    memory_util = Column(BigInteger)

    # degree C card temperature at primary sensor
    temperature = Column(Integer)

    # current power usage in W
    power = Column(BigInteger)

    # power limit in W
    power_limit = Column(BigInteger)

    ce_clock = Column(BigInteger)
    memory_clock = Column(BigInteger)

    time = Column(DateTimeTZAware, default=dt.datetime.now, primary_key=True)

class SampleProcessGpu(TableBase):
    __tablename__ = "sample_process_gpu"

    cluster = Column(String, primary_key=True)
    node = Column(String, primary_key=True)

    job = Column(BigInteger, index=True, primary_key=True)
    epoch = Column(BigInteger,
            desc="Bootcycle presentation of node - continuously increasing number",
            primary_key=True)
    user = Column(String)

    pid = Column(BigInteger, primary_key=True)

    # Card UUID
    uuid = Column(UUID, ForeignKey('sysinfo_gpu_card.uuid'), index=True, primary_key=True)
    index = Column(Integer)

    gpu_util = Column(Float)

    # in kilobytes
    gpu_memory = Column(BigInteger)
    gpu_memory_util = Column(Float)

    time = Column(DateTimeTZAware, default=dt.datetime.now, primary_key=True)

    __table_args__ = (
        Index("ix_sample_process_gpu__cluster_job_time","cluster", "job", time.desc()),
        Index("ix_sample_process_gpu__uuid_time", "uuid", "time"),
        ForeignKeyConstraint(["cluster", "node"], [Node.cluster, Node.node]),
        {
            'info': {'sonar_spec': 'SampleProcess.gpus'},
            'timescaledb_hypertable': {
                'time_column_name': 'time',
                'chunk_time_interval': '24 hours',
                'compression': {
                    'segmentby': 'cluster, job, epoch',
                    'orderby': 'time',
                    'interval': '7 days'
                }
            }

        }
    )

class SampleProcess(TableBase):
    __tablename__ = "sample_process"

    cluster = Column(String, primary_key=True)
    node = Column(String, primary_key=True)

    job = Column(BigInteger, primary_key=True)
    # Slurm jobs will have epoch = 0
    epoch = Column(BigInteger,
            desc="Bootcycle presentation of node - continuously increasing number",
            primary_key=True)

    user = Column(String)

    # private resident memory
    resident_memory = Column(BigInteger, unit='kilobyte')
    # virtual data+stack memory
    virtual_memory = Column(BigInteger, unit='kilobyte')
    # command (not the command line) - '_unknown_' for zombie processes
    cmd = Column(Text)

    # rest of process sample
    pid = Column(BigInteger,
            desc="""
            Process ID, zero is used for rolled-up samples
            """,
            primary_key=True)
    ppid = Column(BigInteger,
            desc="""
            Parent-process ID
            """)

    num_threads = Column(BigInteger,
            desc="""
            The number of threads in the process, minus 1 - we don't count the
            process's main thread (allowing this fields to be omitted in
            transmission for most processes).
            """
            )

    num_threads = Column(Integer,
            desc="""
            Number of threads in the process, minus 1 (main thread is not counted
            """)

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

    data_read = Column(BigInteger,
            desc="""
            Kilobytes read with all sorts of read calls (rounded up).
            """,
            unit='kilobyte')

    data_written = Column(BigInteger,
            desc="""
            Kilobytes written with all sorts of write calls (rounded up).
            """,
            unit='kilobyte')

    data_cancelled = Column(BigInteger,
            desc="""
            Kilobytes written but never flushed to physical media (i.e., held in RAM but then made
            obsolete by overwriting or file deletion or similar) (rounded up).
            """,
            unit='kilebyte')

    # gpus implemented in separate table

    rolledup = Column(Integer,
                      desc="""
                      The number of additional samples for processes that are
                      "the same" that have been rolled into this one. That is,
                      if the value is 1, the record represents the sum of the
                      sample data for two processes."""
               )

    time = Column(DateTimeTZAware, default=dt.datetime.now, primary_key=True)

    __table_args__ = (
        ForeignKeyConstraint(["cluster", "node"], [Node.cluster, Node.node]),
        CheckConstraint("job != 0 or epoch != 0", "job_or_epoch_non_zero"),
        CheckConstraint("job >= 0 and epoch >= 0", "job_or_epoch_non_negative"),
        *ensure_non_negative("pid","ppid"),
        {
            'info': { 'sonar_spec': 'SampleProcess' },
            'timescaledb_hypertable': {
                'time_column_name': 'time',
                'chunk_time_interval': '24 hours',
                'compression': {
                    'segmentby': 'cluster, job, epoch',
                    'orderby': 'time',
                    'interval': '7 days'
                }
            }

        }
    )

class SampleSystem(TableBase):
    __tablename__ = "sample_system"

    cluster = Column(String, primary_key=True)
    node = Column(String, primary_key=True)

    cpus = Column(ARRAY(BigInteger),
                  desc="The state of individual cores"
           )

    used_memory = Column(BigInteger,
                         desc="The amount of primary memory in use in kilobytes",
                         unit="kilobyte"
                  )

    load1 = Column(Float,
                   desc="One-minute load average"
            )

    load5 = Column(Float,
                   desc="Five-minute load average"
            )

    load15 = Column(Float,
                   desc="Fivteen-minute load average"
            )

    runnable_entities = Column(BigInteger,
                            desc="Number of currently runnable scheduling entities (processes, threads)"
                        )
    existing_entities = Column(BigInteger,
                            desc="Number of currently existing scheduling entities (processes, threads)"
                        )

    time = Column(DateTimeTZAware, default=dt.datetime.now, primary_key=True)

    __table_args__ = (
        ForeignKeyConstraint(["cluster", "node"], [Node.cluster, Node.node]),
        *ensure_non_negative("runnable_entities","existing_entities", "used_memory"),
        {
            'info': { 'sonar_spec': 'SampleSystem' },
            'timescaledb_hypertable': {
                'time_column_name': 'time',
                'chunk_time_interval': '24 hours',
                'compression': {
                    'segmentby': 'cluster, node',
                    'orderby': 'time',
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
#                'time_column_name': 'time',
#                'chunk_time_interval': '24 hours',
#                'compression': {
#                    'segmentby': 'job, epoch',
#                    'orderby': 'time',
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


class SlurmJobState(enum.Enum):
    """
    Slurm job states
    see also https://slurm.schedmd.com/job_state_codes.html
    """
    # The key names are relevant here, not the values

    BOOT_FAIL = 'BOOT_FAIL'
    CANCELLED = 'CANCELLED'
    COMPLETED = 'COMPLETED'
    DEADLINE = 'DEADLINE'
    FAILED = 'FAILED'
    NODE_FAIL = 'NODE_FAIL'
    OUT_OF_MEMORY = 'OUT_OF_MEMORY'
    PENDING = 'PENDING'
    PREMPTED = 'PREMPTED'
    RUNNING = 'RUNNING'
    SUSPENDED = 'SUSPENDED'
    TIMEOUT = 'TIMEOUT'


class SampleSlurmJob(TableBase):
    __tablename__ = "sample_slurm_job"
    cluster = Column(String, index=True, primary_key=True)

    # JobID
    # The number of the job or job step. It is in the form: job.jobstep.
    # Meanwhile here - we
    job_id = Column(BigInteger, index=True, primary_key=True)  # ": 244843,
    job_step = Column(String, index=True, primary_key=True)
    job_name = Column(String)
#    job_state = Column(Enum(SlurmJobState))
    job_state = Column(String)

    array_job_id = Column(BigInteger, nullable=True)  # 244843
    array_task_id = Column(Integer, nullable=True)  # 984

    het_job_id = Column(BigInteger)
    het_job_offset = Column(Integer)

    user_name = Column(String)
    account = Column(String)

    start_time = Column(DateTimeTZAware, nullable=True)
    suspend_time = Column(BigInteger)

    submit_time = Column(DateTimeTZAware)
    time_limit = Column(Integer)
    end_time = Column(DateTimeTZAware, nullable=True)
    exit_code = Column(Integer, nullable=True)

    partition = Column(String)
    reservation = Column(String)

    nodes = Column(ARRAY(String))
    priority = Column(Xint)
    distribution = Column(String)

    gres_detail = Column(ARRAY(String), nullable=True)
    requested_cpus = Column(Integer)
    requested_memory_per_node = Column(Integer)
    requested_node_count = Column(Integer)
    minimum_cpus_per_node = Column(Integer)

    time = Column(DateTimeTZAware, default=dt.datetime.now, primary_key=True)

    __table_args__ = (
        *ensure_non_negative(
            'job_id',
            'array_job_id',
            'array_task_id',
            'het_job_id',
            'het_job_offset',
            'requested_cpus',
            'requested_memory_per_node',
            'requested_node_count',
            'minimum_cpus_per_node',
            'suspend_time',
            'exit_code',
        ),
        {
            'info': { 'sonar_spec': 'JobsAttributes.slurm_jobs' },
            'timescaledb_hypertable': {
                'time_column_name': 'time',
                'chunk_time_interval': '24 hours',
                'compression': {
                    'segmentby': 'cluster, job_id',
                    'orderby': 'time',
                    'interval': '7 days'
                }
            }
        }
    )

class SampleSlurmJobAcc(TableBase):
    __tablename__ = "sample_slurm_job_acc"
    cluster = Column(String, index=True, primary_key=True)

    job_id = Column(BigInteger, primary_key=True, index=True)
    job_step = Column(String, index=True, primary_key=True)

    AllocTRES = Column(String)

    ElapsedRaw = Column(BigInteger)

    SystemCPU = Column(BigInteger)
    UserCPU = Column(BigInteger)

    AveVMSize = Column(BigInteger)
    MaxVMSize = Column(BigInteger)

    AveCPU = Column(BigInteger)
    MinCPU = Column(BigInteger)

    AveRSS = Column(BigInteger)
    MaxRSS = Column(BigInteger)

    AveDiskRead = Column(BigInteger)
    AveDiskWrite = Column(BigInteger)

    time = Column(DateTimeTZAware, default=dt.datetime.now, primary_key=True)

    __table_args__ = (
        *ensure_non_negative(
            'job_id',
            #'ElapsedRaw',
            #'SystemCPU',
            #'UserCPU',
            #'AveVMSize',
            #'MaxVMSize',
            #'AveCPU',
            #'MinCPU',
            #'AveRSS',
            #'MaxRSS',
            #'AveDiskRead',
            #'AveDiskWrite'
        ),
        {
            'info': { 'sonar_spec': 'SlurmJob.sacct' },
            'timescaledb_hypertable': {
                'time_column_name': 'time',
                'chunk_time_interval': '24 hours',
                'compression': {
                    'segmentby': 'cluster, job_id',
                    'orderby': 'time',
                    'interval': '7 days'
                }
            }
        }
    )

    ## JobIDRaw
    ## In case of job array print the JobId instead of the ArrayJobId. For non
    ## job arrays the output is the JobId in the format job.jobstep.
    #job_id_raw = Column

    #name = Column(Text)
    #start_time = Column(DateTime, nullable=True)
    #end_time = Column(DateTime, nullable=True)

    #accrue_time = Column(BigInteger)
    #admin_comment = Column(Text)
    #array_max_tasks = Column(Integer)  # 20
    #array_task_string = Column(Text)  #
    #association_id = Column(Integer)  # ": 0,
    ## batch_features": "",
    ## batch_flag": true,
    #batch_host = Column(Text)
    ## flags": [],
    ## burst_buffer": "",
    ## burst_buffer_state": "",

    #cluster = Column(Text)

    ## cluster_features": "",
    ## command": "/global/D1/homes/.."
    ## comment": "",
    ## contiguous": false,
    ## core_spec": null,
    ## thread_spec": null,
    ## cores_per_socket": null,
    ## billable_tres": 1,
    ## cpus_per_task": null,
    ## cpu_frequency_minimum": null,
    ## cpu_frequency_maximum": null,
    ## cpu_frequency_governor": null,
    ## cpus_per_tres": "",
    ## deadline": 0,
    ## delay_boot": 0,
    ## dependency": "",
    #derived_exit_code = Column(BigInteger)  # ": 256,
    #eligible_time = Column(Integer, nullable=True)  # ": 1720736375,

    ## excluded_nodes": "",
    #exit_code = Column(BigInteger)  # ": 0,
    ## features": "",
    ## federation_origin": "",
    ## federation_siblings_active": "",
    ## federation_siblings_viable": "",
    #gres_detail = Column(GPUIdList, default=[])
    #group_id = Column(Integer, nullable=True)  # ": 5000,
    ## job_resources": {
    ## "nodes": "n042",
    ## "allocated_cpus": 1,
    ## "allocated_hosts": 1,
    ## "allocated_nodes": {
    ##   "0": {
    ##     "sockets": {
    ##       "1": "unassigned"
    ##     },
    ##     "cores": {
    ##       "0": "unassigned"
    ##     },
    ##     "memory": 0,
    ##     "cpus": 1
    ##   }
    ## }
    ## ,
    #job_state = Column(Text)  # ": "COMPLETED",
    ## last_sched_evaluation": 1720736375,
    ## licenses": "",
    ## max_cpus": 0,
    ## max_nodes": 0,
    ## mcs_label": "",
    ## memory_per_tres": "",
    ## name": "seidr",
    ## nice": null,
    ## tasks_per_core": null,
    ## tasks_per_node": 0,
    ## tasks_per_socket": null,
    ## tasks_per_board": 0,
    #cpus = Column(Integer)  # 1
    #node_count = Column(Integer)  # 1
    #tasks = Column(Integer)  # 1,
    ## het_job_id": 0,
    ## het_job_id_set": "",
    ## het_job_offset": 0,
    #memory_per_node = Column(Integer)
    #memory_per_cpu = Column(Integer)
    #minimum_cpus_per_node = Column(Integer)
    ## minimum_tmp_disk_per_node": 0,
    ## preempt_time": 0,
    ## pre_sus_time": 0,
    ## priority": 4294726264,
    ## profile": null,
    ## qos": "normal",
    ## reboot": false,
    ## required_nodes": "",
    ## requeue": true,
    ## resize_time": 0,
    ## restart_cnt": 0,
    ## resv_name": "",
    ## shared": null,
    ## show_flags": [
    ## "SHOW_ALL",
    ## "SHOW_DETAIL",
    ## "SHOW_LOCAL"
    ## ,
    ## sockets_per_board": 0,
    ## sockets_per_node": null,
    #state_description = Column(Text)  # "",
    #state_reason = Column(Text)  # "None",
    ## standard_error": "/home/.../scripts/logs/%j-stderr.txt",
    ## standard_input": "/dev/null",
    ## standard_output": "/home/.../scripts/logs/%j-stdout.txt",

    #suspend_time = Column(Integer)  # 0,
    ## system_comment": "",
    ## time_minimum": 0,
    ## threads_per_core": null,
    ## tres_bind": "",
    ## tres_freq": "",
    ## tres_per_job": "",
    ## tres_per_node": "",
    ## tres_per_socket": "",
    ## tres_per_task": "",
    ## tres_req_str": "cpu=1,node=1,billing=1",
    ## tres_alloc_str": "cpu=1,billing=1",
    #user_id = Column(Integer)  # 6500,
    #user_name = Column(Text, nullable=True)
    ## wckey": "",
    ## current_working_directory": "/global/D1/homes/..."
    ## id = Column(Integer) # 244843

    @classmethod
    def from_json(cls, data) -> SampleSlurmJobAcc:
        mapper = class_mapper(cls)
        mapped_data = {}
        for k, v in data.items():
            if k in mapper.column_attrs:
                if type(v) == int and type(cls.__table__.columns[k].type) == DateTime:
                    mapped_data[k] = dt.datetime.fromtimestamp(v, dt.timezone.utc)
                else:
                    mapped_data[k] = v

        return cls(**mapped_data)

class ErrorMessage(TableBase):
    __tablename__ = "error_message"
    __table_args__ = (
        {
            'info': { 'sonar_spec': 'ErrorObject' },
            'timescaledb_hypertable': {
                'time_column_name': 'time',
                'chunk_time_interval': '24 hours',
                'compression': {
                    'segmentby': 'cluster, node',
                    'orderby': 'time',
                    'interval': '7 days'
                }
            }
        }
    )
    cluster = Column(String, primary_key=True, index=True)
    node = Column(String, primary_key=True, index=True)
    detail = Column(Text)

    time = Column(DateTimeTZAware, default=dt.datetime.now, primary_key=True)


# Note: SampleProcess table contains all required information
# class SampleJob(TableBase):

# Note: SlurmJob table contains all required information
# class JobsAttributes(TableBase):
