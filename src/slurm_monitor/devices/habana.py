from __future__ import annotations

import pandas as pd
from io import StringIO
import subprocess

from slurm_monitor.utils import utcnow
from slurm_monitor.devices.gpu import (
    GPU,
    GPUInfo,
    GPUProcessStatus,
    GPUStatus
)

import logging

logger = logging.getLogger(__name__)

class Habana(GPU):
    @classmethod
    def detect(cls) -> GPUInfo:
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
                    memory_total=memory.total, # in bytes
                    framework=GPUInfo.Framework.HABANA,
                    versions=versions
            )
        except ImportError:
            logger.debug("pyhlml - failed to import - trying with hl-smi")

        response = subprocess.run("command -v hl-smi", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if response.returncode != 0:
            raise RuntimeError("hl-smi is not available")

        # Examples output:
        # HL-205, 32768
        result = subprocess.run("hl-smi --query-aip=name,memory.total --format=csv,nounits,noheader",
                shell=True, stdout=subprocess.PIPE, stderr=None)
        model_infos = result.stdout.decode("UTF-8").strip().split("\n")
        if len(model_infos) > 0:
            fields = model_infos[0].split(",")
            return GPUInfo(
                    model=fields[0].strip(),
                    count=len(model_infos),
                    memory_total=int(fields[1].strip())*1024**2, # in bytes
                    framework=GPUInfo.Framework.HABANA,
                    versions=versions
            )
        raise ValueError("No Intel (Habana) GPU found")

    @property
    def query_cmd(self):
        return "hl-smi"

    @property
    def query_argument(self):
        return "--format=csv,nounits --query-aip"

    @property
    def query_properties(self):
        return {
                "name": "name",
                "uuid": "uuid",
                "power.draw": "power.draw [W]",
                "temperature.aip": "temperature.aip [C]",
                "utilization.aip": "utilization.aip [%]",
                "memory.used": "memory.used [MiB]",
                #'memory.free',
                # extra
                "memory.total": "memory.total [MiB]"
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
                power_draw=value[ query_properties["power.draw"] ],
                temperature_gpu=value[ query_properties["temperature.aip"] ],
                utilization_memory=int(value[ query_properties["memory.used"] ])
                * 100.0
                / int(value[ query_properties["memory.total"] ]),
                utilization_gpu=value[query_properties["utilization.aip"]],
                memory_total=int(value[query_properties["memory.total"]])*1024**2, # in bytes
                timestamp=timestamp,
            )
            samples.append(sample)

        return samples

    def get_processes(self) -> list[GPUProcessStatus]:
        samples = []
        return samples
