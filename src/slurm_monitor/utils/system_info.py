from __future__ import annotations

import platform
import subprocess
from enum import Enum
from pathlib import Path
import os
import sys
import multiprocessing
import json
import argparse

import logging

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
        # TODO: consider lscpu
        if Path("/proc/cpuinfo").exists():
            return os.popen("cat /proc/cpuinfo | grep 'model name' | uniq | cut -d: -f2").read().strip()

        raise RuntimeError("Failed to locate /proc/cpuinfo")

    def get_cpu_count(self) -> int:
        cpu_count = multiprocessing.cpu_count()
        if "SLURM_CPUS_ON_NODE" in os.environ:
            cpu_count = int(os.environ["SLURM_CPUS_ON_NODE"])
        return cpu_count

    def __iter__(self):
        yield "model", self.model
        yield "count", self.count


class GPUInfo:
    class Framework(str, Enum):
        UNKNOWN = "unknown"
        CUDA = "cuda"
        ROCM = "rocm"
        HABANA = "habana"

    model: str | None = None
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

    @classmethod
    def detect(cls) -> GPUInfo:
        for i in ["nvidia", "amd", "intel"]:
            try:
                query_fn = f"get_{i}_gpus"
                if not hasattr(cls, query_fn):
                    raise RuntimeError(f"Missing query function for {i} gpus")

                gpu_info = getattr(cls,query_fn)()
                if gpu_info.count > 0:
                    return gpu_info

            except Exception as e:
                logging.debug(f"No {i} gpu - {e}")

        return GPUInfo()

    @classmethod
    def get_intel_gpus(cls) -> tuple[str, int]:
        versions = {}
        try:
            import pyhlml
            pyhlml.hlmlInit()

            device_count = pyhlml.hlmlDeviceGetCount()
            if device_count < 1:
                raise ValueError("No Intel (Habana) GPU found")

            device = pyhlml.hlmlDeviceGetHandleByIndex(0)
            model_name = pyhlml.hlmlDeviceGetName(device).decode("UTF-8").strip()
            memory = pyhlml.hlmlDeviceGetMemoryInfo(device)
            pyhlml.hlmlShutdown()
            return GPUInfo(
                    model=model_name,
                    count=device_count,
                    memory_total=memory.total,
                    framework=cls.Framework.HABANA,
                    versions=versions
            )
        except ImportError:
            logger.debug("pyhlml - failed to import - trying with hl-smi")

        response = subprocess.run("command -v hl-smi", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if response.returncode != 0:
            raise RuntimeError("hl-smi is not available")

        result = subprocess.run("hl-smi --query-aip=name,memory.total --format=csv,nouints,noheader",
                shell=True, stdout=subprocess.PIPE, stderr=None)
        model_infos = result.stdout.decode("UTF-8").strip().split("\n")
        if len(model_infos) > 0:
            fields = model_infos[0].split(",")
            return GPUInfo(
                    model=fields[0].strip(),
                    count=len(model_infos),
                    memory_total=int(fields[1].strip()),
                    framework=cls.Framework.HABANA,
                    versions=versions
            )
        raise ValueError("No Intel (Habana) GPU found")

    @classmethod
    def get_nvidia_gpus(cls) -> GPUInfo:
        if "CUDA_ROOT" in os.environ:
            version_json = Path(os.environ['CUDA_ROOT']) / "version.json"
            if version_json.exists():
                with open(version_json, "r") as f:
                    versions = json.load(f)
        try:
            import pynvml
            pynvml.nvmlInit()
            device_count = pynvml.nvmlDeviceGetCount()
            if device_count < 1:
                raise ValueError("No Nvida GPU found")

            device = pynvml.nvmlDeviceGetHandleByIndex(0)
            model = pynvml.nvmlDeviceGetName(device)
            memory = pynvml.nvmlDeviceGetMemoryInfo(device)

            return GPUInfo(
                    model=model,
                    count=device_count,
                    memory_total=memory.total,
                    framework=cls.Framework.CUDA,
                    versions=versions
            )
        except ImportError:
            logger.debug("pynvml - failed to import - trying with nvidia-smi, rocm-smi and others")

        response = subprocess.run("command -v nvidia-smi", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if response.returncode != 0:
            raise RuntimeError("nvidia-smi is not available")

        ids = ""
        if "CUDA_VISIBLE_DEVICES" in os.environ:
            ids += f"-i {os.environ['CUDA_VISIBLE_DEVICES']}"

        result = subprocess.run(f"nvidia-smi {ids} --query-gpu=gpu_name,memory.total --format=noheader,csv,nounits",
                shell=True, stdout=subprocess.PIPE, stderr=None)
        if result.returncode != 0:
            raise RuntimeError("nvidia-smi is not usable")

        models = result.stdout.decode("UTF-8").strip().split("\n")
        if models:
            model, memory_total = models[0].split(',')
            return GPUInfo(
                    model=model.strip(),
                    count=len(models),
                    memory_total=int(memory_total.strip()),
                    framework=cls.Framework.CUDA,
                    versions=versions
                    )

        raise ValueError("No Nvida GPU found")

    @classmethod
    def get_amd_gpus(cls) -> GPUInfo:
        versions = {}
        if "ROCm_ROOT" in os.environ:
            for file in (Path(os.environ['ROCm_ROOT']) / ".info").glob("version*"):
                with open(file, "r") as f:
                    versions[file.name] = f.read().strip()

        try:
            from pyrsmi import rocml
            rocml.smi_initialize()
            device_count = rocml.smi_get_device_count()
            if device_count < 1:
                raise ValueError("No AMD GPU found")

            model = rocml.smi_get_device_name(0)
            memory_total = rocml.smi_get_device_memory_total(0)
            rocml.smi_shutdown()
            return GPUInfo(
                    model=model,
                    count=device_count,
                    memory_total=int(memory_total/(1024**2)),
                    framework=cls.Framework.ROCM,
                    versions=versions
            )
        except ImportError:
            logger.debug("pyrsmi - failed to import - trying with rocm-smi")

        response = subprocess.run("command -v rocm-smi", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if response.returncode != 0:
            raise RuntimeError("rocm-smi is not available")

        result = subprocess.run("rocm-smi --showproductname --showmeminfo vram --csv",
                shell=True, stdout=subprocess.PIPE, stderr=None)
        info = result.stdout.decode("UTF-8").strip().split("\n")
        if len(info) > 1:
            fields = info[1].split(",")
            return GPUInfo(
                    model=fields[3],
                    count=len(info)-1 ,
                    memory_total=int(fields[1])/(1024**2),
                    framework=cls.Framework.ROCM,
                    versions=versions
            )

        raise ValueError("No AMD GPU found")


class SystemInfo:
    cpu_info: CPUInfo
    gpu_info: GPUInfo | None

    env: dict[str,str] = {}
    platform_params: dict[str,str] = {}

    def __init__(self):
        self.gpu_info = GPUInfo.detect()
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

        if args and type(args) == argparse.Namespace:
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
