from __future__ import annotations

from enum import Enum
import logging
import socket
from typing import abstractmethod
from pydantic import BaseModel
import datetime as dt

from slurm_monitor.utils.command import Command

logger = logging.getLogger(__name__)

# from .db_tables import GPUs, GPUStatus
class GPUStatus(BaseModel):
    uuid: str
    node: str
    model: str
    local_id: int
    # memory total in bytes
    memory_total: int

    temperature_gpu: float
    power_draw: float
    utilization_gpu: float
    utilization_memory: float

    pstate: str | None = None
    timestamp: str | dt.datetime


class GPUProcessStatus(BaseModel):
    uuid: str
    pid: int
    process_name: str

    # utilization in percent of stream multiprocessors
    utilization_sm: float
    # used memory in bytes
    used_memory: int


class GPU():
    node: str
    _uuids: list[str]

    def __init__(self):
        self.node = socket.gethostname()
        self._uuids = []

    @property
    def uuids(self) -> list[str]:
        if not self._uuids:
            self._uuids = [x.uuid for x in self.get_status()]
        return self._uuids

    @property
    def query_cmd(self):
        return "nvidia-smi"

    @property
    def smi_query_statement(self):
        return f"{self.query_cmd} {self.query_argument}={','.join(self.query_properties.keys())}"

    def query_status_smi(self) -> str:
        return Command.run(self.smi_query_statement)

    @property
    @abstractmethod
    def query_argument(self):
        raise NotImplementedError("Please implement 'GPU.query_argument'")

    @property
    @abstractmethod
    def query_properties(self) -> dict[str, list]:
        """
        Map of query property name, and the corresponding headers in the output
        """
        raise NotImplementedError("Please implement 'GPU.query_properties'")

    def transform(self, response: str) -> list[GPUStatus]:
        raise NotImplementedError("Please implement 'GPU.transform'")

    def get_status(self) -> list[GPUStatus]:
        response = self.query_status_smi()
        return self.transform(response)

    def get_processes(self) -> list[GPUProcessStatus]:
        raise NotImplementedError("Please implement 'GPU.get_processes'")


class GPUInfo:
    class Framework(str, Enum):
        UNKNOWN = "unknown"
        CUDA = "cuda"
        ROCM = "rocm"
        HABANA = "habana"
        XPU = "xpu"

    model: str | None = None
    # in bytes
    memory_total: int = 0
    count: int = 0
    framework: Framework | None = None

    versions: dict[str,any] = {}
    def __init__(self,
            model: str | None = None,
            count: int = 0,
            memory_total: int = 0,
            framework: Framework = Framework.UNKNOWN,
            versions: dict[str,any] = {}
            ):

        self.model = model
        self.count = count
        self.memory_total = memory_total
        self.framework = framework
        self.versions = versions

    def __iter__(self):
        yield "model", self.model
        yield "count", self.count
        yield "memory_total", self.memory_total
        yield "framework", self.framework.value
        yield "versions", self.versions
