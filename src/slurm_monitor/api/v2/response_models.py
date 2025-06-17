from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    model_validator,
    RootModel
)
from typing import TypeVar, Generic

UUID = str

# Here we defined the response model for the REST API
# There is a strong similarity with the sqlalchemy tables types,
# but their might be deviations - mainly to augment fields
# or to facilitate processing.
#
# Augmented / computed fields will be marked as 'computed field'

class SimpleModel(BaseModel):
    # https://docs.pydantic.dev/2.10/api/config/#pydantic.config.ConfigDict.from_attributes
    model_config = ConfigDict(from_attributes=True)

class TimestampedModel(SimpleModel):
    time: AwareDatetime

class ClusterResponse(TimestampedModel):
    cluster: str
    slurm: int
    partitions: list[str]
    nodes: list[str]

class SAcctResponse(BaseModel):
    AllocTRES: str
    ElapsedRaw: int

    SystemCPU: int
    UserCPU: int

    AveVMSize: int
    MaxVMSize: int

    AveCPU: int
    MinCPU: int

    AveRSS: int
    MaxRSS: int

    AveDiskRead: int
    AveDiskWrite: int

class JobResponse(TimestampedModel):
    cluster: str
    job_id: int
    job_step: str
    job_name: str
    job_state: str

    array_job_id: int | None = Field(default=None)
    array_task_id: int | None = Field(default=None)

    het_job_id: int
    het_job_offset: int
    user_name: str
    account: str

    # Pending jobs might have no start time set
    start_time: AwareDatetime | None = Field(default=None)
    suspend_time: int
    submit_time: AwareDatetime
    time_limit: int
    end_time: AwareDatetime | None = Field(default=None)
    exit_code: int | None = Field(default=None)

    partition: str
    reservation: str
    nodes: list[str]
    reservation: str
    nodes: list[str]
    priority: int
    distribution: str

    gres_detail: list[str] | None = Field(default=None)
    requested_cpus: int
    requested_memory_per_node: int
    requested_node_count: int
    minimum_cpus_per_node: int

    # computed field: list of the actually used GPU uuids
    #    what can be oberved in the process data)
    used_gpu_uuids: list[str] | None = Field(default=None)

    sacct: SAcctResponse | None = Field(default=None)


class PartitionResponse(TimestampedModel):
    cluster: str
    name: str
    nodes: list[str]
    nodes_compact: list[str]

    # computed field: list of jobs that have status PENDING
    jobs_pending: list[JobResponse]
    # computed field: list of jobs that have status RUNNING
    jobs_running: list[JobResponse]
    # computed field: the time of the longest job in PENDING
    pending_max_submit_time: AwareDatetime
    # computed field: the time from submit to start for job in RUNNING
    running_latest_wait_time: int

    # computed field: total number of cpus (sum of node cpus)
    total_cpus: int
    # computed field: total number of gpus (sum of node gpus)
    total_gpus: int
    # computed field: number of gpus that are reserved (from slurm jobs)
    gpus_reserved: int
    # computed field: list of uuids that are in use, actually observed
    gpus_in_use: list[str]

class JobsResponse(BaseModel):
    jobs: list[JobResponse]

class NodeStateResponse(TimestampedModel):
    cluster: str
    node: str
    states: list[str]

class GPUCardResponse(TimestampedModel):
    uuid: str
    manufacturer: str
    model: str
    architecture: str
    memory: int

    cluster: str
    node: str
    index: int
    address: str
    driver: str
    firmware: str
    power_limit: int
    max_power_limit: int
    min_power_limit: int
    max_ce_clock: int
    max_memory_clock: int

class SampleGpuBaseResponse(TimestampedModel):
    failing: int
    fan: float
    compute_mode: str
    performance_state: float
    memory: float
    memory_util: float
    memory_clock: float
    ce_util: float
    ce_clock: float
    temperature: float
    power: float
    power_limit: float

class SampleGpuResponse(SampleGpuBaseResponse):
    uuid: str
    index: int

class SampleGpuTimeseriesResponse(BaseModel):
    """
    Used GPU uuid and (local) index to identify the GPU and 
    provide the timeseries of samples
    """
    uuid: str
    index: int
    data: list[SampleGpuBaseResponse]

class SampleProcessGpuResponse(TimestampedModel):
    cluster: str
    node: str
    job: int
    epoch: int
    user: str
    pid: int
    uuid: str
    index: int

    gpu_util: float
    gpu_memory: float
    gpu_memory_util: float

class SampleProcessGpuAccResponse(TimestampedModel):
    gpu_memory: float
    gpu_util: float
    gpu_memory_util: float
    pids: list[int]

class SampleProcessResponse(TimestampedModel):
    cluster: str
    node: str
    job: int
    epoch: int
    user: str

    resident_memory: int
    virtual_memory: int
    cmd: str
    pid: int
    ppid: int

    cpu_avg: float
    cpu_util: float
    cpu_time: float

    rolled_up: int


class SampleProcessAccResponse(TimestampedModel):
    memory_resident: float
    memory_virtual: float
    memory_util: float

    cpu_avg: float
    cpu_util: float
    cpu_time: float

    processes_avg: float

class NodeInfoResponse(TimestampedModel):
    cluster: str
    node: str
    os_name: str
    os_release: str
    architecture: str
    sockets: int
    cores_per_socket: int
    threads_per_core: int
    cpu_model: str
    memory: int
    topo_svg: str | None
    cards: list[GPUCardResponse]

    # augmented field: partitions this node is part of
    partitions: list[str]


T = TypeVar('T')
class JobSpecificTimeseriesResponse(BaseModel, Generic[T]):
    job: int
    epoch: int
    data: list[T]

class PidTimeseriesResponse(BaseModel, Generic[T]):
    pid: int
    data: list[T]

class GpusProcessTimeSeriesResponse(BaseModel):
    gpus: dict[UUID, list[SampleProcessGpuAccResponse]]

class CPUMemoryProcessTimeSeriesResponse(BaseModel):
    cpu_memory: list[SampleProcessAccResponse]

class CombinedProcessTimeSeriesResponse(CPUMemoryProcessTimeSeriesResponse, GpusProcessTimeSeriesResponse):
    pass

class SystemProcessTimeseriesResponse(BaseModel):
    job: int
    epoch: int
    nodes: dict[str, CombinedProcessTimeSeriesResponse]

class NodeJobSampleProcessTimeseriesResponse(BaseModel):
    job: int
    epoch: int
    processes: list[PidTimeseriesResponse[SampleProcessAccResponse]]

class JobNodeSampleProcessGpuTimeseriesResponse(BaseModel):
    job: int
    epoch: int
    nodes: dict[str, GpusProcessTimeSeriesResponse]

class JobNodeSampleProcessTimeseriesResponse(BaseModel):
    job: int
    epoch: int
    nodes: dict[str, CPUMemoryProcessTimeSeriesResponse]


#### GPU
# Note: using RootModel, we can describ dictionary with out
#    requiring a top-level key

# Uuid top-level
class GpuJobSampleProcessGpuTimeseriesResponse(BaseModel):
    # gpus: { uuid : list[job-timeseries] }
    gpus: dict[str, list[JobSpecificTimeseriesResponse]]

# Node top-level
class NodeGpuJobSampleProcessGpuTimeseriesResponse(RootModel):
    root: dict[str, GpuJobSampleProcessGpuTimeseriesResponse]

class NodeGpuTimeseriesResponse(RootModel):
    """
        List of timeseries data per GPU
    """
    root: dict[str, list[SampleGpuTimeseriesResponse]]

class NodeSampleProcessGpuAccResponse(RootModel):
    root: dict[str, dict[str, SampleProcessGpuAccResponse]]

    @model_validator(mode="before")
    def check_nested_structure(cls, values) -> dict[str, dict[str, SampleProcessGpuAccResponse]]:
        if not isinstance(values, dict):
            raise ValueError(f"Response must be a dictionary: keys are nodenames - response was {type(values)}")
        for key, gpus in values.items():
            if not isinstance(gpus, dict):
                raise ValueError(f"Nodes need to map to dict of gpu-uuid to timeseries data, but {key=} maps to {gpus=}")
            for gpu_uuid, gpu_data in gpus.items():
                if not isinstance(gpu_data, SampleProcessGpuAccResponse):
                    raise ValueError(f"GPU data should be SampleProcessGpuAccResponse, but {gpu_uuid=} maps to {gpu_data=}")
        return values


class NodeSampleProcessAccResponse(RootModel):
    root: dict[str, dict[str, SampleProcessAccResponse]]

    @model_validator(mode="before")
    def check_nested_structure(cls, values) -> dict[str, dict[str, SampleProcessGpuAccResponse]]:
        if not isinstance(values, dict):
            raise ValueError(f"Response must be a dictionary: keys are nodenames - response was {type(values)}")
        return values

## DASHBOARD
#export interface FetchedJobQueryResultItem {
#  job: string;
#  user: string;
#  host: string;
#  duration: string;
#  start: string;
#  end: string;
#  'cpu-peak': number;
#  'res-peak': number;
#  'mem-peak': number;
#  'gpu-peak': number;
#  'gpumem-peak': number;
#  cmd: string;
#}

class JobQueryResultItem(BaseModel):
    job: str
    user: str
    host: str
    duration: str
    start: str
    end: str
    cmd: str

    cpu_peak: float = Field(default=0.0, serialization_alias='cpu-peak')
    res_peak: float = Field(default=0.0, serialization_alias='res-peak')
    mem_peak: float = Field(default=0.0, serialization_alias='mem-peak')
    gpu_peak: float = Field(default=0.0, serialization_alias='gpu-peak')
    gpumem_peak: float = Field(default=0.0, serialization_alias='gpumem-peak')


#export interface FetchedJobProfileResultItem {
#  time: string;
#  job: number;
#  points: ProcessPoint[];
#}
#
#interface ProcessPoint {
#  command: string;
#  pid: number;
#  cpu: number;
#  mem: number;
#  res: number;
#  gpu: number;
#  gpumem: number;
#  nproc: number;
#}

class ProcessPoint(BaseModel):
    command: str
    pid: int
    nproc: int

    cpu: float
    mem: float
    res: float

    gpu: float
    gpumem: float

class JobProfileResultItem(BaseModel):
    job: int
    time: str

    points: list[ProcessPoint]

class ErrorMessageResponse(BaseModel):
    cluster: str
    node: str

    details: str
    time: str

class QueriesResponse(BaseModel):
    queries: list[str]
