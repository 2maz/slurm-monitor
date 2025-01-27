from __future__ import annotations

import pandas as pd
from io import StringIO
import os
import json
import subprocess

from slurm_monitor.utils import utcnow
from slurm_monitor.utils.command import Command
from slurm_monitor.devices.gpu import GPU, GPUInfo, GPUProcessStatus, GPUStatus

import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class Nvidia(GPU):
    @property
    def query_argument(self):
        return "--format=csv,nounits --query-gpu"

    @classmethod
    def detect(cls) -> GPUInfo:
        versions = {}
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
            model = pynvml.nvmlDeviceGetName(device).decode('UTF-8')
            memory = pynvml.nvmlDeviceGetMemoryInfo(device)

            return GPUInfo(
                    model=model,
                    count=device_count,
                    memory_total=memory.total, # in bytes
                    framework=GPUInfo.Framework.CUDA,
                    versions=versions
            )
        except ImportError as e:
            logger.debug(f"{cls}.detect: failed to import {e}")
        except Exception as e:
            logger.debug(f"{cls}.detect: failed to extract information - {e}")

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
            model, memory_total_in_MB = models[0].split(',')
            return GPUInfo(
                    model=model.strip(),
                    count=len(models),
                    memory_total=int(memory_total_in_MB.strip())*1024**2,
                    framework=GPUInfo.Framework.CUDA,
                    versions=versions
                    )

        raise ValueError("No Nvida GPU found")


    @property
    def query_args(self):
        return "--query-gpu"

    @property
    def query_properties(self):
        return {
                "name" : "name",
                "uuid" : "uuid",
                "power.draw": "power.draw [W]",
                "temperature.gpu" : "temperature.gpu",
                "utilization.gpu": "utilization.gpu [%]",  # Percent of time over the past sample
                # period during which one or more kernels was executing on the GPU.
                "utilization.memory": "utilization.memory [%]",  # Percent of time over the past sample
                # period during which global (device) memory was being read or written.
                "memory.used" : "memory.used [MiB]",
                "memory.free" : "memory.free [MiB]",
                # extra
                #'pstate',
        }

    def transform(self, response: str) -> list[GPUStatus]:
        df = pd.read_csv(StringIO(response.strip()))
        column_names = { x: x.strip() for x in df.columns }
        df.rename(columns = column_names, inplace = True)

        df.uuid = df.uuid.str.strip()

        records = df.to_dict('records')

        samples = []
        timestamp = utcnow()
        query_properties = self.query_properties

        for idx, value in enumerate(records):
            sample = GPUStatus(
                model=value[ query_properties["name"] ],
                uuid=value[ query_properties["uuid"] ],
                local_id=idx,
                node=self.node,
                power_draw=value[query_properties["power.draw"]],
                temperature_gpu=value[ query_properties["temperature.gpu"]],
                utilization_memory=value[ query_properties["utilization.memory"] ],
                utilization_gpu=value[query_properties["utilization.gpu"]],
                memory_total=(
                    int(value[query_properties["memory.used"]])
                    + int(value[query_properties["memory.free"]])
                    )*1024**2, # in bytes
                timestamp=timestamp,
            )
            samples.append(sample)
        return samples

    def get_processes(self) -> list[GPUProcessStatus]:
        # header gpu_uuid, pid, process_name, used_gpu_memory [MiB]
        response = Command.run(f"{self.query_cmd} "
                        "--query-compute-apps=gpu_uuid,pid,name,used_gpu_memory "
                        "--format=csv,nounits"
                )
        df = pd.read_csv(StringIO(response.strip()))

        column_names = { x: x.strip() for x in df.columns }
        df.rename(columns = column_names, inplace = True)
        df.gpu_uuid = df.gpu_uuid.str.strip()

        records = df.to_dict('records')

        samples = []
        utcnow()

        for idx, value in enumerate(records):
            sample = GPUProcessStatus(
                    uuid=value["gpu_uuid"],
                    pid=int(value["pid"]),
                    process_name=value["process_name"].strip(),
                    utilization_sm=0, # TBD
                    used_memory=value["used_gpu_memory [MiB]"]*1024**2
                    )
            samples.append(sample)
        return samples
