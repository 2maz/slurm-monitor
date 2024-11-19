from __future__ import annotations

import pandas as pd
from io import StringIO
import os
import re
import subprocess
from pathlib import Path

from slurm_monitor.utils import utcnow
from slurm_monitor.utils.command import Command
from slurm_monitor.devices.gpu import GPU, GPUInfo, GPUProcessStatus, GPUStatus

import logging

logger = logging.getLogger(__name__)

class ROCM(GPU):
    _uuids: list[str]

    def __init__(self):
        super().__init__()

        self._uuids = []

    @classmethod
    def detect(cls):
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
                raise RuntimeError("ROCM.detect: no GPU found")

            model = rocml.smi_get_device_name(0)
            memory_total_in_bytes = rocml.smi_get_device_memory_total(0)
            rocml.smi_shutdown()
            return GPUInfo(
                    model=model,
                    count=device_count,
                    memory_total=memory_total_in_bytes,
                    framework=GPUInfo.Framework.ROCM,
                    versions=versions
            )
        except ImportError as e:
            logger.debug(f"{cls}.detect: failed to import {e}")
        except Exception as e:
            logger.debug(f"{cls}.detect: failed to extract information - {e}")

        response = subprocess.run("command -v rocm-smi", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if response.returncode != 0:
            raise RuntimeError("rocm-smi is not available")

        result = subprocess.run("rocm-smi --showproductname --showmeminfo vram --csv",
                shell=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        info = result.stdout.decode("UTF-8").strip().split("\n")
        if len(info) > 1:
            fields = info[1].split(",")
            return GPUInfo(
                    model=fields[3],
                    count=len(info)-1 ,
                    memory_total=int(fields[1]), # in bytes
                    framework=GPUInfo.Framework.ROCM,
                    versions=versions
            )

        raise RuntimeError("ROCM.detect: no AMD GPU found")

    @property
    def smi_query_statement(self) -> str:
        return f"{self.query_cmd} {self.query_argument}"

    @property
    def query_cmd(self):
        return "rocm-smi"

    @property
    def query_argument(self):
        # from https://github.com/ROCm/rocm_smi_lib/tree/master/python_smi_tools
        # showmeminfo: vram, vis_vram, gtt
        #     vram: Video RAM or graphicy memory
        #     vis_vram: visible VRAM - CPU accessible video memory
        #     gtt: Graphics Translation Table
        #     all: all of the above
        #
        # --show-productname -> Card series,Card model,Card vendor,Card SKU
        #
        # "device",
        # "Unique ID",  # uuid
        # "Temperature (Sensor edge) (C)",  # 'temperature.sensor_edge
        # "Temperature (Sensor junction) (C)",  # temperature.sensor_junction
        # "Temperature (Sensor memory) (C)",  # temperature.sensor_memory
        # # optional
        # "Temperature (Sensor HBM 0) (C)",  # temperature.sensor_hbm_0
        # "Temperature (Sensor HBM 1) (C)",  # temperature.sensor_hbm_1
        # "Temperature (Sensor HBM 2) (C)",  # temperature.sensor_hbm_2
        # "Temperature (Sensor HBM 3) (C)",  # temperature.sensor_hbm_3
        # "Average Graphics Package Power (W)",  # power.draw
        # "GPU use (%)",  # utilization.gpu
        # "GFX Activity",  # utilization.gfx,
        # "GPU memory use (%)",  # utilization.memory / high or low (1 or 0)
        # "Memory Activity",
        # "Voltage (mV)",
        # "VRAM Total Memory (B)",  # memory.total
        # "VRAM Total Used Memory (B)",  # memory used
        # "Card series",
        # "Card model",
        # "Card vendor",
        # "Card SKU"
        return "--showuniqueid --showproductname --showuse --showmemuse \
                --showmeminfo vram --showvoltage --showtemp --showpower --csv"

    def transform(self, response: str) -> list[GPUStatus]:
        main_response = [x for x in response.strip().split("\n") if not x.lower().startswith("warn")]

        df = pd.read_csv(StringIO('\n'.join(main_response)))
        column_names = { x: x.strip().lower() for x in df.columns }
        df.rename(columns = column_names, inplace = True)

        records = df.to_dict('records')

        samples = []
        timestamp = utcnow()


        for idx, value in enumerate(records):
            total_memory_in_bytes = int(value["vram total memory (b)"])
            sample = GPUStatus(
                model=value["card series"],
                uuid=value["unique id"],
                local_id=idx,
                node=self.node,
                power_draw=value["average graphics package power (w)"],
                temperature_gpu=value["temperature (sensor edge) (c)"],
                utilization_memory=int(value["vram total used memory (b)"])*100.0/total_memory_in_bytes,
                utilization_gpu=value["gpu use (%)"],
                memory_total=total_memory_in_bytes,
                timestamp=timestamp,
            )
            samples.append(sample)

        return samples

    @property
    def uuids(self) -> list[str]:
        if not self._uuids:
            self._uuids = [x.uuid for x in self.get_status()]
        return self._uuids


    def get_processes(self) -> list[GPUProcessStatus]:
        response = Command.run("rocm-smi --showpidgpus")
        pid = None
        local_ids = []
        for x in response.strip().split("\n"):
            if pid is not None:
                try:
                    local_ids = [int(i.strip()) for i in x.strip().split(" ")]
                except Exception:
                    logger.debug("ROCM.get_processes: pid known, but GPU id are not yet registered")
                # local ids for this pid have been processed, so reset
                pid = None

            if x.lower().startswith("warn"):
                continue

            m = re.match(r"PID ([0-9]+) is using", x)
            if m:
                pid = int(m.group(1))

        response = Command.run("rocm-smi --showpids --csv")
        main_response = [x for x in response.strip().split("\n") if "warn" not in x.lower()]
        samples = []
        for line in main_response[1:]:
            line = line.replace("\"","").replace(", ",",")
            try:
                fields = line.split(",")
                if not fields[0].startswith("PID"):
                    raise ValueError("Expected PID prefix")

                pid = int(fields[0].replace("PID",""))
                for local_id in local_ids:
                    sample = GPUProcessStatus(
                        uuid=self.uuids[local_id],
                        pid=pid,
                        process_name=fields[1].strip(),
                        #number_of_gpus=int(fields[2]),
                        used_memory=int(fields[3]), # vram_used
                        #sdma_used=int(fields[4]),
                        utilization_sm=float(fields[5].strip()),
                    )
                    samples.append(sample)
            except Exception as e:
                logger.debug(e)
        return samples
