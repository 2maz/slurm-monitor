import subprocess
from threading import Thread
import time
import datetime as dt
from abc import abstractmethod
from queue import Queue

import logging
import re

from slurm_monitor.db.db import SlurmMonitorDB, DatabaseSettings
from slurm_monitor.db.db_tables import GPUStatus, Nodes

logger = logging.getLogger(__name__)


class GPUObserver:
    _samples: Queue

    def __init__(self):
        self._samples = Queue()

    def notify(self, samples: list[GPUStatus]):
        [self._samples.put(x) for x in samples]


class GPUObservable:
    observers: list[GPUObserver]

    def __init__(self, observers: list[GPUObserver] = []):
        self.observers = observers

    def add_observer(self, observer: GPUObserver):
        if observer not in self.observers:
            self.observers.append(observer)

    def notify_all(self, samples: list[GPUStatus]):
        [x.notify(samples) for x in self.observers]


class DataCollector:
    thread: Thread
    _stop: bool = False

    sampling_interval_in_s: int

    def __init__(self, sampling_interval_in_s: int):
        self.sampling_interval_in_s = sampling_interval_in_s
        self.thread = Thread(target=self._run, args=())

    @staticmethod
    def get_user():
        return (
            subprocess.run("whoami", stdout=subprocess.PIPE)
            .stdout.decode("utf-8")
            .strip()
        )

    def start(self):
        self._stop = False
        self.thread.start()

    def _run(self):
        start_time = None
        while not self._stop:
            if (
                start_time
                and (dt.datetime.utcnow() - start_time).total_seconds()
                < self.sampling_interval_in_s
            ):
                time.sleep(1)
            else:
                data = self.run()
                self.notify(data=data)
                start_time = dt.datetime.utcnow()

    def stop(self):
        self._stop = True

    def notify(self, data):
        pass


class GPUInfoCollector(DataCollector, GPUObservable):
    nodename: str
    user: str = None
    gpu_type: str

    def __init__(self, nodename: str, gpu_type: str, user: str = None):
        super().__init__(sampling_interval_in_s=10)
        self.observers = []
        self.local_id_mapping = {}

        if user is None:
            self.user = self.get_user()
        self.nodename = nodename

    def run(self) -> dict[str, any]:
        response = self.send_request(nodename=self.nodename, user=self.user)
        return {self.nodename: {"gpus": self.parse_response(response)}}

    def send_request(self, nodename: str, user: str) -> str:
        msg = f"ssh {user}@{nodename} '{self.query_cmd} {self.query_argument} {','.join(self.query_properties)}'"
        return subprocess.run(msg, shell=True, stdout=subprocess.PIPE).stdout.decode(
            "utf-8"
        )

    @property
    @abstractmethod
    def query_properties(self) -> list[str]:
        raise NotImplementedError("Please implement 'query_properties'")

    def parse_response(self, response: str) -> dict[str]:
        gpus = []
        for line in response.strip().split("\n"):
            gpu_data = {}
            for idx, field in enumerate(line.split(",")):
                value = field.strip()
                gpu_data[self.query_properties[idx]] = value
            gpus.append(gpu_data)
        return gpus

    def notify(self, data):
        samples = []
        timestamp = dt.datetime.utcnow()

        for idx, value in enumerate(data[self.nodename]["gpus"]):
            sample = GPUStatus(
                name=value["name"],
                uuid=value["uuid"],
                local_id=idx,
                node=self.nodename,
                power_draw=value["power.draw"],
                temperature_gpu=value["temperature.gpu"],
                utilization_memory=value["utilization.memory"],
                utilization_gpu=value["utilization.gpu"],
                memory_total=value["memory.used"] + value["memory.free"],
                timestamp=timestamp,
            )
            samples.append(sample)

        self.notify_all(samples)

    def get_local_id_mapping(self) -> dict[str, int]:
        msg = f"ssh {self.user}@{self.nodename} '{self.query_cmd} -L'"
        response = subprocess.run(
            msg, shell=True, stdout=subprocess.PIPE
        ).stdout.decode("utf-8")

        mapping = {}
        for line in response.strip().split("\n"):
            # example: GPU 0: Tesla V100-SXM3-32GB (UUID: GPU-ad466f2f-575d-d949-35e0-9a7d912d974e)
            m = re.match(r"GPU ([0-9]+): [^(]+ \(UUID: (.*)\)", line)
            uuid = m.group(2)
            local_id = int(m.group(1))
            mapping[uuid] = local_id
        return mapping


class NvidiaInfoCollector(GPUInfoCollector):
    def __init__(self, nodename: str, user: str = None):
        super().__init__(nodename=nodename, gpu_type="nvidia", user=user)

    @property
    def query_cmd(self):
        return "nvidia-smi"

    @property
    def query_argument(self):
        return "--format=csv,nounits,noheader --query-gpu"

    @property
    def query_properties(self):
        return [
            "name",
            "uuid",
            "power.draw",
            "temperature.gpu",  #
            "utilization.gpu",  # Percent of time over the past sample
            # period during which one or more kernels was executing on the GPU.
            "utilization.memory",  # Percent of time over the past sample
            # period during which global (device) memory was being read or written.
            "memory.used",
            "memory.free",
            # extra
            #'pstate',
        ]


class HabanaInfoCollector(GPUInfoCollector):
    def __init__(self, nodename: str, user: str = None):
        super().__init__(nodename=nodename, gpu_type="habana", user=user)

    @property
    def query_cmd(self):
        return "hl-smi"

    @property
    def query_argument(self):
        return "--format=csv,nounits,noheader --query-aip"

    @property
    def query_properties(self):
        return [
            "name",
            "uuid",
            "power.draw",
            "temperature.aip",
            "utilization.aip",
            "memory.used",
            #'memory.free',
            # extra
            "memory.total",
        ]

    def notify(self, data):
        samples = []
        timestamp = dt.datetime.utcnow()
        for idx, value in enumerate(data[self.nodename]["gpus"]):
            sample = GPUStatus(
                name=value["name"],
                uuid=value["uuid"],
                local_id=idx,
                node=self.nodename,
                power_draw=value["power.draw"],
                temperature_gpu=value["temperature.aip"],
                utilization_memory=int(value["memory.used"])
                * 1.0
                / int(value["memory.total"]),
                utilization_gpu=value["utilization.aip"],
                memory_total=value["memory.total"],
                timestamp=timestamp,
            )
            samples.append(sample)

        self.notify_all(samples)


class ROCMInfoCollector(GPUInfoCollector):
    def __init__(self, nodename: str, user: str = None):
        super().__init__(nodename=nodename, gpu_type="amd", user=user)

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
        return "--showuniqueid --showuse --showmemuse --showmeminfo vram --showvoltage --showtemp --showpower --csv"

    @property
    def query_properties(self):
        return [
            "device",
            "Unique ID",  # uuid
            "Temperature (Sensor edge)",  # 'temperature.sensor_edge
            "Temperature (Senors junction)",  # temperature.sensor_junction
            "Temperature (Sensor memory)",  # temperature.sensor_memory
            "Temperature (Sensor HBM 0)",  # temperature.sensor_hbm_0
            "Temperature (Sensor HBM 1)",  # temperature.sensor_hbm_1
            "Temperature (Sensor HBM 2)",  # temperature.sensor_hbm_2
            "Temperature (Sensor HBM 3)",  # temperature.sensor_hbm_3
            "Average Graphics Package Power (W)",  # power.draw
            "GPU use (%)",  # utilization.gpu
            "GFX Activity",  # utilization.gfx,
            "GPU Memory use (%)",  # utilization.memory / high or low (1 or 0)
            "Memory Activity",
            "Voltage (mV)",
            "VRAM Total Memory (B)",  # memory.total
            "VRAM Total Used (B)",  # memory.used
        ]

    def send_request(self, nodename: str, user: str) -> str:
        msg = f"ssh {user}@{nodename} '{self.query_cmd} {self.query_argument}'"
        return subprocess.run(msg, shell=True, stdout=subprocess.PIPE).stdout.decode(
            "utf-8"
        )

    def parse_response(self, response: str) -> dict[str]:
        gpus = []
        for line in response.strip().split("\n")[1:]:
            gpu_data = {}
            for idx, field in enumerate(line.split(",")):
                value = field.strip()
                gpu_data[self.query_properties[idx]] = value
            gpus.append(gpu_data)
        return gpus

    def notify(self, data):
        samples = []
        timestamp = dt.datetime.utcnow()

        for idx, value in enumerate(data[self.nodename]["gpus"]):
            sample = GPUStatus(
                name=value["device"],
                uuid=value["Unique ID"],
                local_id=idx,
                node=self.nodename,
                power_draw=value["Average Graphics Package Power (W)"],
                temperature_gpu=value["Temperature (Sensor edge)"],
                utilization_memory=int(value["VRAM Total Used (B)"])
                * 1.0
                / int(value["VRAM Total Memory (B)"]),
                utilization_gpu=value["GPU use (%)"],
                memory_total=value["VRAM Total Memory (B)"],
                timestamp=timestamp,
            )
            samples.append(sample)
        self.notify_all(samples)

    def get_local_id_mapping(self) -> dict[str, int]:
        msg = f"ssh {self.user}@{self.nodename} '{self.query_cmd} --showuniqueid --csv'"
        response = subprocess.run(
            msg, shell=True, stdout=subprocess.PIPE
        ).stdout.decode("utf-8")
        # Example:
        #   device,Unique ID
        #   card0,0x18f68e602b8a790f
        #   card1,0x95a1ca7691e7c391

        mapping = {}
        for line in response.strip().split("\n")[1:]:
            m = re.match(r"card([0-9]+),(.*)", line)
            local_id = m.group(1)
            uuid = m.group(2)
            mapping[uuid] = local_id

        return mapping


class CollectorPool(GPUObserver):
    _collectors: list[GPUInfoCollector]
    db: SlurmMonitorDB
    _stop: bool = False

    def __init__(self, db: SlurmMonitorDB | None = None):
        super().__init__()

        self._collectors = []
        self.monitor_thread = Thread(target=self._run, args=())
        self.save_thread = Thread(target=self._save, args=())

        if db is None:
            db_settings = DatabaseSettings(
                user="", password="", uri="sqlite:////tmp/slurm-monitor-db.sqlite"
            )
            self.db = SlurmMonitorDB(db_settings=db_settings)
        else:
            self.db = db

    def add_collector(self, collector: GPUInfoCollector):
        if collector not in self._collectors:
            collector.add_observer(self)
            self._collectors.append(collector)

    def _run(self):
        [x.start() for x in self._collectors]
        [x.thread.join() for x in self._collectors]

    def _save(self):
        while not self._stop:
            sample: GPUStatus = self._samples.get()
            self.db.insert_or_update(Nodes(name=sample.node))
            self.db.insert(sample)

    def start(self):
        self._stop = False

        self.monitor_thread.start()
        self.save_thread.start()

    def stop(self):
        self._stop = True

        for x in self._collectors:
            x.stop()

    def join(self):
        self.monitor_thread.join()
        self.save_thread.join()

def run():
    db_settings = DatabaseSettings()
    pool = CollectorPool(db=SlurmMonitorDB(db_settings=db_settings))

if __name__ == "__main__":
    pool = CollectorPool()
    collectors = [
        NvidiaInfoCollector(nodename="g001"),
        NvidiaInfoCollector(nodename="g002"),
        NvidiaInfoCollector(nodename="n009"),
        NvidiaInfoCollector(nodename="n010"),
        NvidiaInfoCollector(nodename="n011"),
        NvidiaInfoCollector(nodename="n012"),
        NvidiaInfoCollector(nodename="n013"),
        NvidiaInfoCollector(nodename="n014"),
        HabanaInfoCollector(nodename="h001"),
        ROCMInfoCollector(nodename="n015"),
        ROCMInfoCollector(nodename="n016"),
    ]

    [pool.add_collector(x) for x in collectors]

    pool.start()
    while True:
        print(f"queue size: {pool._samples.qsize()} {dt.datetime.now()}\r", end="")
        time.sleep(5)

    pool.join()

if __name__ == "__main__":
    run()
