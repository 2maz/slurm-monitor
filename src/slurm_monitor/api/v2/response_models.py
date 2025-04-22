from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    model_validator,
    RootModel
)
import datetime as dt
from typing import TypeVar, Generic

UUID = str

class SimpleModel(BaseModel):
    # https://docs.pydantic.dev/2.10/api/config/#pydantic.config.ConfigDict.from_attributes
    model_config = ConfigDict(from_attributes=True)

class TimestampedModel(SimpleModel):
    time: dt.datetime

class ClusterResponse(TimestampedModel):
    cluster: str
    slurm: int
    partitions: list[str]
    nodes: list[str]

class JobResponse(TimestampedModel):
    cluster: str
    job_id: int
    job_step: str
    job_name: str
    job_state: str

    array_job_id: int | None
    array_task_id: int | None

    het_job_id: int
    het_job_offset: int
    user_name: str
    account: str

    start_time: dt.datetime
    suspend_time: int
    submit_time: dt.datetime
    time_limit: int
    end_time: dt.datetime | None
    exit_code: int | None

    partition: str
    reservation: str
    nodes: list[str]
    reservation: str
    nodes: list[str]
    priority: int
    distribution: str

    gres_detail: list[str] | None
    requested_cpus: int
    requested_memory_per_node: int
    requested_node_count: int
    minimum_cpus_per_node: int


class PartitionResponse(TimestampedModel):
   cluster: str
   name: str
   nodes: list[str]
   nodes_compact: list[str]

   jobs_pending: list[JobResponse]
   jobs_running: list[JobResponse]
   pending_max_submit_time: dt.datetime
   running_latest_wait_time: int
   total_cpus: int
   total_gpus: int
   gpus_reserved: int
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

class SampleGpuResponse(TimestampedModel):
    uuid: str
    index: int
    failing: int
    fan: int
    compute_mode: str
    performance_state: int
    memory: int
    ce_util: int
    memory_util: int
    temperature: int
    power: int
    power_limit: int
    memory_clock: int

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
    gpu_memory: int
    gpu_memory_util: float

class SampleProcessGpuAccResponse(TimestampedModel):
    gpu_memory: int
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
    cpu_time: int

    rolled_up: int


class SampleProcessAccResponse(TimestampedModel):
    memory_resident: int
    memory_virtual: int
    memory_util: float

    cpu_avg: float
    cpu_util: float
    cpu_time: int

    processes_avg: int

class NodeResponse(TimestampedModel):
    cluster: str
    node: str
    os_name: str
    os_release: str
    architecture: str
    sockets: int
    cores_per_socket: int
    threads_per_core: int
    cpu_model: str
    description: str
    memory: int
    topo_svg: str | None
    cards: list[GPUCardResponse]
    #
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
# Uuid top-level
class GpuJobSampleProcessGpuTimeseriesResponse(RootModel):
    # uuid > list[job-timeseries]
    root: dict[str, list[JobSpecificTimeseriesResponse]]

# Node top-level
class NodeGpuJobSampleProcessGpuTimeseriesResponse(RootModel):
    root: dict[str, GpuJobSampleProcessGpuTimeseriesResponse]

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
                if not isinstance(gpu_data, dict):
                    raise ValueError(f"GPU data needs to be a single accumulated data response, but {gpu_uuid=} maps to {gpu_data=}")
        return values



