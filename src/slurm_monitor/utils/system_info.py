from __future__ import annotations

import platform
import logging
import os
import sys
import argparse
import multiprocessing

from slurm_monitor.utils.command import Command
from slurm_monitor.devices import detect_gpus
from slurm_monitor.devices.gpu import GPU, GPUInfo

logger = logging.getLogger(__name__)

class CPUInfo:
    model: str = None
    count: int = 0

    def __init__(self, model: str | None = None, count: str | None = None):
        if model is None:
            self.model = self.get_cpu_model()
        else:
            self.model = model

        if count is None:
            self.count = self.get_cpu_count()
        else:
            self.count = count

    def get_cpu_model(self) -> str:
        return Command.run("lscpu | grep -i 'model name' | uniq | cut -d: -f2 | xargs")

    def get_cpu_count(self, reserved: bool = False) -> int:
        if reserved and "SLURM_CPUS_ON_NODE" in os.environ:
            return int(os.environ["SLURM_CPUS_ON_NODE"])

        return multiprocessing.cpu_count()

    def __iter__(self):
        yield "model", self.model
        yield "count", self.count


class SystemInfo:
    cpu_info: CPUInfo
    gpu_info: GPUInfo | None

    env: dict[str,str] = {}
    platform_params: dict[str,str] = {}

    # Query object for the gpu, if available
    _gpu: GPU | None

    def __init__(self):
        self.gpu_info, self._gpu = detect_gpus()
        self.cpu_info = CPUInfo()

        self.platform_params = self.get_platform_params()

        prefixes = ["SLURM_", "CUDA_", "ROCR_"]
        for prefix in prefixes:
            self.env.update({ k: v for k,v in os.environ.items() if k.startswith(prefix)})

    def __iter__(self):
        yield "cpus", dict(self.cpu_info)
        yield "gpus", dict(self.gpu_info)
        yield "env", self.env
        yield "platform", self.platform_params

    def mlflow_log(self, args: argparse.Namespace | dict[str|str] | None = None):
        """
            Helper method to log information with mlflow
        """
        import mlflow

        mlflow.log_params(self.get_platform_params())
        mlflow.log_param("SYSTEM_INFO__CPU_MODEL", self.cpu_info.model)
        mlflow.log_param("SYSTEM_INFO__CPU_COUNT", self.cpu_info.count)

        mlflow.log_param("SYSTEM_INFO__GPU_MODEL", self.gpu_info.model)
        mlflow.log_param("SYSTEM_INFO__GPU_COUNT", self.gpu_info.count)
        if self.env:
           mlflow.log_params(self.env)

        mlflow.log_param("SYSTEM_INFO__ARGV", sys.argv)
        mlflow.log_param("SYSTEM_INFO__CWD", os.getcwd())

        if args and type(args) is argparse.Namespace:
            arguments = {f"ARG__{x}": getattr(args, x) for x in args.__dict__}
            mlflow.log_params(arguments)

    def get_platform_params(self) -> dict[str, any]:
        if not self.platform_params:
            for i in ["system", "processor", "platform", "machine", "version", "node", "python_version"]:
                self.platform_params[i] =  getattr(platform,i)()
        return self.platform_params

    def get_descriptor(self) -> str:
        name = f"[arch:{self.platform_params['machine']}]"
        name += f"[cpu:{self.cpu_info.model}:{self.cpu_info.count}]"
        if self.gpu_info:
            if self.gpu_info.count > 0:
                name += f"[gpu:{self.gpu_info.model.replace(' ','-')}:{self.gpu_info.count}]"
            else:
                name += "[gpu:0]"
        return name
