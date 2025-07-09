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
    time: AwareDatetime = Field(description="Timezone Aware timestamp")

class ClusterResponse(TimestampedModel):
    cluster: str = Field(description="Name of the cluster")
    slurm: int = Field(description="Whether SLURM is in use (1) or not (0)")
    partitions: list[str] = Field(description="List of available partitions in this cluster")
    nodes: list[str] = Field(description="List of available nodes in this cluster")

doc_sacct = "(see <a href='https://slurm.schedmd.com/sacct.html#SECTION_Job-Accounting-Fields'> SLURM Job Accounting</a>)"
class SAcctResponse(BaseModel):
    f"""Slurm Job Accounting Response {doc_sacct}"""

    AllocTRES: str = Field(description=f"Allocated Trackable resources (TRES) {doc_sacct}")
    ElapsedRaw: int = Field(description=f"The job's elapsed time in seconds {doc_sacct}")

    SystemCPU: int = Field(description=f"The amount of CPU time used by the job or job step. {doc_sacct}")
    UserCPU: int = Field(description=f"The amount of user CPU time used by the job or job step. {doc_sacct}")

    AveVMSize: int = Field(description=f"Average virtual memory size {doc_sacct}")
    MaxVMSize: int = Field(description=f"Maximum virtual memory size {doc_sacct}")

    AveCPU: int = Field(description=f"Average CPU usage {doc_sacct}")
    MinCPU: int = Field(description=f"Minimum CPU usage {doc_sacct}")

    AveRSS: int = Field(description=f"Average resident set size of all tasks in job {doc_sacct}")
    MaxRSS: int = Field(description=f"Maximum resident set size of all tasks in job {doc_sacct}")

    AveDiskRead: int = Field(description=f"Average number of bytes read by all tasks in job {doc_sacct}")
    AveDiskWrite: int = Field(description=f"Average number of bytes written by all tasks in job {doc_sacct}")

class JobResponse(TimestampedModel):
    cluster: str = Field(description='Name of the cluster')

    job_id: int = Field(description='Identifier of the SLURM job')
    job_step: str = Field(description="""
        The step identifier for the job identified by job_id.  For the topmost step/stage of a job
        this will be the empty string.  Other values normally have the syntax of unsigned integers,
        but may also be the strings "extern" and "batch".  This field's default value is the empty
        string.

        NOTE: step 0 and step "empty string" are different, in fact thinking of a normal number-like
        step name as a number may not be very helpful.
        """)
    job_name: str = Field(description='Name of the job')
    job_state: str = Field(description="State of the job, e.g., PENDING, RUNNING, FAILED (see <a href='https://slurm.schedmd.com/job_state_codes.html#states'>SLURM Job States</a>)")

    array_job_id: int | None = Field(default=None, description="""
        The overarching ID of an array job, see discussion in the postamble.

        sacct: the n of a `JobID` of the form `n_m.s`

        slurm: `JOB_INFO.array_job_id`.
        """)
    array_task_id: int | None = Field(default=None, description="""
        if `array_job_id` is not zero, the array element's index.  Individual elements of an array
        job have their own plain job_id; the `array_job_id` identifies these as part of the same array
        job and the array_task_id identifies their position within the array, see later discussion.

        sacct: the m of a `JobID` of the form `n_m.s`.

        slurm: `JOB_INFO.array_task_id`.
        """)

    het_job_id: int = Field(description="Id of the het(erogeneous) job (see <a href='https://slurm.schedmd.com/heterogeneous_jobs.html'>SLURM documentation</a>)")
    het_job_offset: int = Field(description="Unique sequence number (starting with 0) the het(erogeneous) job component (see <a href='https://slurm.schedmd.com/heterogeneous_jobs.html'>SLURM documentation</a>)")

    user_name: str = Field(description="Name of the user that started the job")
    account: str = Field(description="Name of the account")

    # Pending jobs might have no start time set
    start_time: AwareDatetime | None = Field(default=None, description="Time at which the job started - only present if the job started")
    suspend_time: int = Field(description="Time the job was suspended in seconds")
    submit_time: AwareDatetime = Field(description="Time at which the job was submitted")
    time_limit: int = Field(description="Time limit for this job in seconds")
    end_time: AwareDatetime | None = Field(default=None, description="Time at which the job ended - only present if the job ended")
    exit_code: int | None = Field(default=None, description="Exit code of the job - given it has finished")

    partition: str = Field(description="Name of the partition this job is associated with")
    reservation: str
    nodes: list[str] = Field(description="List of nodes that are requested by this job")
    reservation: str
    priority: int
    distribution: str

    gres_detail: list[str] | None = Field(default=None, description="List of general resources details")
    requested_cpus: int = Field(description="Number of requested CPUs")
    requested_memory_per_node: int = Field(description="Requested memory in bytes per node")
    requested_node_count: int = Field(description="Requested number of nodes")
    minimum_cpus_per_node: int = Field(description="Minimum required CPUs per node")

    # computed field: list of the actually used GPU uuids
    #    what can be oberved in the process data)
    used_gpu_uuids: list[str] | None = Field(default=None, description="UUIDs of GPUs that are actually used with this job - this might be different to the number of reserved GPUs (this a field computed by slurm-monitor)")

    sacct: SAcctResponse | None = Field(default=None, description="Slurm Accounting Response data (<a href='https://slurm.schedmd.com/sacct.html#SECTION_Job-Accounting-Fields'>Slurm documentation</a>)")


class PartitionResponse(TimestampedModel):
    cluster: str = Field(description="Name of the cluster")
    name: str = Field(description="Name of the partition")
    nodes: list[str] = Field(description="Nodes associated with this partition")
    nodes_compact: list[str] = Field(description="A compact representation of the list of nodes, e.g., n001,n002,n003 is n001-003")

    # computed field: list of jobs that have status PENDING
    jobs_pending: list[JobResponse] = Field(description="List of pending jobs in this partition (computed by slurm-monitor)")
    # computed field: list of jobs that have status RUNNING
    jobs_running: list[JobResponse] = Field(description="List of running jobs in this partition (computed by slurm-monitor)")
    # computed field: the time of the longest job in PENDING
    pending_max_submit_time: AwareDatetime = Field(description="Timestamp of the job being longest in PENDING state")
    # computed field: the time from submit to start for job in RUNNING
    running_latest_wait_time: int = Field(description="Waiting time in seconds of the most recent started job in this partition")

    # computed field: total number of cpus (sum of node cpus)
    total_cpus: int = Field(description="Total number of CPUs available in this partition")
    # computed field: total number of gpus (sum of node gpus)
    total_gpus: int = Field(description="Total number of GPUs available in this partition")
    # computed field: number of gpus that are reserved (from slurm jobs)
    gpus_reserved: int = Field(description="Total number of GPUs that are currently reserved in this partition")
    # computed field: list of uuids that are in use, actually observed
    gpus_in_use: list[str] = Field(description="UUIDs of gpus that are currently in use in this partition")

class JobsResponse(BaseModel):
    jobs: list[JobResponse] = Field(description="List of jobs")

class NodeStateResponse(TimestampedModel):
    cluster: str = Field(description="Name of the cluster")
    node: str = Field(description="Name of the node")
    states: list[str] = Field(description="State(s) this node is currently in")

class GPUCardResponse(TimestampedModel):
    uuid: str = Field(description="UUID as reported by card")
    manufacturer: str = Field(description="A manufacturer: 'NVIDIA', 'AMD', 'INTEL' (other TBD)")
    model: str = Field(description="card dependent, manufacturer's model string")
    architecture: str = Field(description="card-dependent, manufacturer's arch string, for NVIDIA this is 'Turing', 'Volta', etc.")

    cluster: str = Field(description="Name of the cluster that this GPU currently belongs to")
    node: str = Field(description="Name of the node that this GPU belongs to")
    index: int = Field(description="Local card index, may change at boot")
    address: str = Field(description="Indicates an intra-system card address, e.g., PCI adress")
    driver: str = Field(description="card-dependent, the manufacturer's driver string")
    firmware: str = Field(description="card-dependant, the manufacturer's firmware string")

    max_power_limit: int = Field(description="card-dependent, max power the card can draw in W(atts)")
    min_power_limit: int = Field(description="card-dependent: min power the card will draw in W(atts)")
    max_ce_clock: int = Field(description="card-dependent, maximum clock of compute element")
    max_memory_clock: int = Field(description="card-dependent, maximum clock of GPU memory")

class SampleGpuBaseResponse(TimestampedModel):
    failing: int = Field(description="If not zero and error code indicating a card failure state. Code=1 is 'generic failure'. Other codes TBD")
    fan: float = Field(description="Percent of primary fan's max speed, max exceed 100% on some cards in some cases")
    compute_mode: str = Field(description="card-dependent, current compute mode if known")
    performance_state: float = Field(description="Current performance level, card-specific >= 0, or unset for 'unknown'")
    memory: float = Field(description="Memory use in KiB")
    memory_util: float = Field(description="Memory used in percentage")
    memory_clock: float = Field(description="Memory current clock")

    ce_util: float = Field(description="Compute element capability used in percentage")
    ce_clock: float = Field(description="Compute element current clock")

    temperature: float = Field(description="Card temperature a primar sensor in degrees Celsius (can be negative")
    power: float = Field(description="Current power usage in W(atts")
    power_limit: float = Field(description="Current power limit in W(atts)")

class SampleGpuResponse(SampleGpuBaseResponse):
    uuid: str = Field(description="UUID of the GPU")
    index: int = Field(description="Local index of the GPU")

class SampleGpuTimeseriesResponse(BaseModel):
    """
    Used GPU uuid and (local) index to identify the GPU and
    provide the timeseries of samples
    """
    uuid: str = Field(description="UUID of the GPU")
    index: int = Field(description="Local index of the GPU")
    data: list[SampleGpuBaseResponse] = Field(description="Timeseries of SampleGpu for this GPU")

class SampleProcessGpuResponse(TimestampedModel):
    cluster: str = Field(description="Name of the cluster with GPU of given UUID")
    node: str = Field(description="Name of the node with the GPU of given UUID")
    job: int = Field(description="Job that started the process that is sampled")
    epoch: int = Field(description="Epoch identifying the job")
    user: str = Field(description="User running the job")
    pid: int = Field(description="Id of the process that is sampled")
    uuid: str = Field(description="GPU UUID which is sampled")
    index: int = Field(description="Local index of the GPU")

    gpu_util: float = Field(description="GPU Compute utilization in percentage")
    gpu_memory: float = Field(description="GPU Memory being utilized in KiB")
    gpu_memory_util: float = Field(description="GPU Memory utilization in percentage")

class SampleProcessGpuAccResponse(TimestampedModel):
    gpu_memory: float = Field(description="GPU Memory being utilized in KiB")
    gpu_util: float = Field(description="GPU Compute utilization in percentage")
    gpu_memory_util: float = Field(description="GPU Memory utilization in percentage")

    pids: list[int] = Field(description="Process ids related to an accumulated sample")

doc_sample_process = "(see <a href='https://github.com/NordicHPC/sonar/blob/main/util/formats/newfmt/types.go#L632'>SampleProcess</a>)"
class SampleProcessResponse(TimestampedModel):
    cluster: str = Field(description="Name of the cluster")
    node: str = Field(description="Name of the node")
    job: int = Field(description="Job id")
    epoch: int = Field(description="Epoch to uniquely identify non-slurm jobs")
    user: str = Field(description="User running the job")

    resident_memory: int = Field(description="Resident memory used in KiB")
    virtual_memory: int = Field(description="Virtual memory used in KiB")
    cmd: str = Field(description="The command associate with this process")
    pid: int = Field(description="Process id of the sampled process - zero for rolled up samples")
    ppid: int = Field(description="Parent process id of the sampled process")

    cpu_avg: float = Field(description=f"The running average CPU over the true lifetime of the process {doc_sample_process}")
    cpu_util: float = Field(description=f"The current CPU utilization in percentage {doc_sample_process}")
    cpu_time: float = Field(description=f"Cumulative CPU time in seconds of the full lifetime of the process {doc_sample_process}")

    num_threads: int = Field(description=f"Number of threads in the process minus 1 - (main thread is not counted) {doc_sample_process}")

    rolled_up: int = Field(
        description=f"""The number of additional samples for processes that are "the same" that have been rolled into
        this one. That is, if the value is 1, the record represents the sum of the sample data for
        two processes. {doc_sample_process}"""
    )

class SampleProcessAccResponse(TimestampedModel):
    memory_resident: float = Field(description="Current resident memory usage in KiB")
    memory_virtual: float = Field(description="Current virtual memory usage in KiB")
    memory_util: float = Field(description="Current Memory utilization in percentage")

    cpu_avg: float = Field(description="Average CPU utilization over the lifetime of the accumulated processes")
    cpu_util: float = Field(description="Current CPU utilization in percentage")
    cpu_time: float = Field(description="Total CPU time in seconds for the lifetime of the related processes")

    # computed field
    processes_avg: float = Field(description="Average number of processes running for this accumulated response")

class NodeInfoResponse(TimestampedModel):
    cluster: str = Field(description="Name of the cluster")
    node: str = Field(description="Name of the node")
    os_name: str = Field(description="Name of the operating system")
    os_release: str = Field(description="Name of the specific release of the operating system")
    architecture: str = Field(description="Architecture of this node")
    sockets: int = Field(description="Number of CPU sockets available")
    cores_per_socket: int = Field(description="Number of core per socket")
    threads_per_core: int = Field(description="Number of threads per core")
    cpu_model: str = Field(description="CPU model specifier")
    memory: int = Field(description="Available memory in this node in KiB")
    topo_svg: str | None = Field(description="The architecture topography (lstopo) as svg")
    cards: list[GPUCardResponse] = Field(description="List of GPUs")

    # augmented field: partitions this node is part of
    partitions: list[str] = Field(description="List of partitions that this node belongs to")


T = TypeVar('T')
class JobSpecificTimeseriesResponse(BaseModel, Generic[T]):
    job: int = Field(description="Job ID")
    epoch: int = Field(description="Epoch uniquely identifying non-slurm jobs")
    data: list[T]

class PidTimeseriesResponse(BaseModel, Generic[T]):
    pid: int = Field(description="Process ID")
    data: list[T]

class GpusProcessTimeSeriesResponse(BaseModel):
    gpus: dict[UUID, list[SampleProcessGpuAccResponse]]

class CPUMemoryProcessTimeSeriesResponse(BaseModel):
    cpu_memory: list[SampleProcessAccResponse]

class CombinedProcessTimeSeriesResponse(CPUMemoryProcessTimeSeriesResponse, GpusProcessTimeSeriesResponse):
    pass

class SystemProcessTimeseriesResponse(BaseModel):
    job: int = Field(description="Job ID")
    epoch: int = Field(description="Epoch uniquely identifying non-slurm jobs")
    nodes: dict[str, CombinedProcessTimeSeriesResponse]

class NodeJobSampleProcessTimeseriesResponse(BaseModel):
    job: int = Field(description="Job ID")
    epoch: int = Field(description="Epoch uniquely identifying non-slurm jobs")
    processes: list[PidTimeseriesResponse[SampleProcessAccResponse]]

class JobNodeSampleProcessGpuTimeseriesResponse(BaseModel):
    job: int = Field(description="Job ID")
    epoch: int = Field(description="Epoch uniquely identifying non-slurm jobs")
    nodes: dict[str, GpusProcessTimeSeriesResponse]

class JobNodeSampleProcessTimeseriesResponse(BaseModel):
    job: int = Field(description="Job ID")
    epoch: int = Field(description="Epoch uniquely identifying non-slurm jobs")
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
    cluster: str = Field(description="Name of the cluster")
    node: str = Field(description="Name of the node (in the cluster)")

    details: str = Field(description="Details of the reported error")
    time: str = Field(description="Time at which this error occurred")

class QueriesResponse(BaseModel):
    queries: list[str] = Field(description=f"List of available predefined queries")
