from __future__ import annotations

import pandas as pd
from io import StringIO
import subprocess
import json

from slurm_monitor.utils.command import Command
from slurm_monitor.utils import utcnow, ensure_float
from slurm_monitor.devices.gpu import GPU, GPUInfo, GPUStatus

import logging

logger = logging.getLogger(__name__)

class XPU(GPU):
    devices: dict[str,any]

    def __init__(self):
        super().__init__()
        self.devices = self.discover()

    @property
    def smi_query_statement(self):
        return f"{self.query_cmd} {self.query_argument}"

    @classmethod
    def detect(cls) -> GPUInfo:
        versions = {}
        response = subprocess.run("command -v xpu-smi", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if response.returncode != 0:
            raise RuntimeError("xpu-smi is not available")

        devices = cls.discover()

        if not devices:
            raise ValueError("No Intel (XPU) GPU found")

        device_data = list(devices.values())[0]
        return GPUInfo(
                model=device_data['device_name'],
                count=len(devices),
                memory_total=ensure_float(device_data, 'memory_physical_size_byte', 0)/(1024.0**2),
                framework=GPUInfo.Framework.XPU,
                versions=versions
        )

    @classmethod
    def discover(cls):
        devices_json = Command.run("xpu-smi discovery -j")
        devices_data = json.loads(devices_json)

        devices = {}

        if "device_list" in devices_data:
            # mapping local id to the device data, e.g.,
            # {
            #     "device_function_type": "physical",
            #     "device_id": 0,
            #     "device_name": "Intel(R) Data Center GPU Max 1100",
            #     "device_type": "GPU",
            #     "drm_device": "/dev/dri/card1",
            #     "pci_bdf_address": "0000:29:00.0",
            #     "pci_device_id": "0xbda",
            #     "uuid": "00000000-0000-0029-0000-002f0bda8086",
            #     "vendor_name": "Intel(R) Corporation"
            # },
            for device_data in devices_data["device_list"]:
                device_id = device_data['device_id']

                device_json = Command.run(f"xpu-smi discovery -d {device_id} -j")
                devices[device_id] = json.loads(device_json)
        return devices

    @property
    def query_cmd(self):
        return "xpu-smi dump"

    def get_gpu_status(self) -> str:
        return Command.run(f"{self.query_cmd} {self.query_argument}")

    @property
    def query_argument(self):
        return "-i1 -n1 -d'-1' -m 0,1,2,3,4,5,18"

    @property
    def query_properties(self):
        return [
            "Timestamp",
            "DeviceId",
            "GPU Utilization (%)",
            "GPU Power (W)",
            "GPU Frequency (MHz)",
            "GPU Core Temperature (Celsius Degree)",
            "GPU Memory Temperature (Celsius Degree)",
            "GPU Memory Utilization (%)",
            "GPU Memory Used (MiB)"
        ]

    def transform(self, response: str) -> list[GPUStatus]:
        df = pd.read_csv(StringIO(response.strip()))
        column_names = { x: x.strip() for x in df.columns }
        df.rename(columns = column_names, inplace = True)

        records = df.to_dict('records')

        samples = []
        timestamp = utcnow()
        for idx, value in enumerate(records):
            device_data = self.devices[idx]
            sample = GPUStatus(
                model=device_data["device_name"],
                uuid=device_data["uuid"],
                local_id=idx,
                node=self.node,
                power_draw=ensure_float(value, "GPU Power (W)", 0),
                temperature_gpu=ensure_float(value, "GPU Core Temperature (Celsius Degree)", 0),
                utilization_memory=ensure_float(value, "GPU Memory Utilization (%)", 0),
                utilization_gpu=ensure_float(value, "GPU Utilization (%)", 0),
                memory_total=ensure_float(device_data, "memory_physical_size_byte", 0)/(1024.0**2), # MB
                timestamp=timestamp,
            )
            samples.append(sample)

        return samples
