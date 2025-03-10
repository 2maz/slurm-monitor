from argparse import ArgumentParser
import subprocess
from pathlib import Path
import sys
from threading import Thread
import time
import datetime as dt
from abc import abstractmethod
from queue import Queue, Empty
from typing import Generic, TypeVar

import logging
import re

from slurm_monitor.utils import utcnow
from .db import SlurmMonitorDB, DatabaseSettings, Database
from .db_tables import GPUStatus, JobStatus, Nodes

from slurm_monitor.utils.slurm import Slurm

logger = logging.getLogger(__name__)

T = TypeVar("T")

class Observer(Generic[T]):
    _samples: Queue

    def __init__(self):
        self._samples = Queue()

    def notify(self, samples: list[T]):
        [self._samples.put(x) for x in samples]


class Observable(Generic[T]):
    observers: list[Observer[T]]

    def __init__(self, observers: list[Observer[T]] = []):
        self.observers = observers

    def add_observer(self, observer: Observer[T]):
        if observer not in self.observers:
            self.observers.append(observer)

    def notify_all(self, samples: list[T]):
        [x.notify(samples) for x in self.observers]


class DataCollector(Observer[T]):
    thread: Thread
    _stop: bool = False

    # adapts dynamically
    sampling_interval_in_s: int
    # for resetting
    _sampling_interval_in_s: int

    name: str

    def __init__(self, name: str, sampling_interval_in_s: int):
        self.name = name
        self.sampling_interval_in_s = sampling_interval_in_s
        self._sampling_interval_in_s = sampling_interval_in_s
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
                and (dt.datetime.now() - start_time).total_seconds()
                < self.sampling_interval_in_s
            ):
                time.sleep(1)
            else:
                try:
                    samples: list[T] = self.run()
                    self.notify_all(samples=samples)

                    # reset sampling interval upon successful retrieval
                    self.sampling_interval_in_s = self._sampling_interval_in_s
                except Exception as e:
                    # Dynamically increase the sampling interval for failing nodes, but cap at
                    # 15 min
                    self.sampling_interval_in_s = min(
                        self.sampling_interval_in_s * 2, 15 * 60
                    )
                    logger.warning(
                        f"{self.name}: failed to collect data."
                        f" Increasing sampling interval to {self.sampling_interval_in_s} s."
                        f" -- details: {e}"
                    )

                start_time = dt.datetime.now()

    def stop(self):
        self._stop = True

    def join(self):
        self.thread.join()

    def notify(self, data: list[T]):
        pass


class GPUStatusCollector(DataCollector[GPUStatus], Observable[GPUStatus]):
    nodename: str
    user: str = None
    gpu_type: str

    def __init__(self, nodename: str, gpu_type: str, user: str = None, sampling_interval_in_s: int | None = None):
        if sampling_interval_in_s is None:
            sampling_interval_in_s = 20

        super().__init__(name=f"gpu-collector-{nodename}", sampling_interval_in_s=sampling_interval_in_s)

        self.observers = []
        self.local_id_mapping = {}

        if user is None:
            self.user = self.get_user()
        self.nodename = nodename

    def run(self) -> list[GPUStatus]:
        response = self.send_request(nodename=self.nodename, user=self.user)
        if response == "" or response is None:
            raise ValueError(f"GPUStatusCollector: {self.nodename}: No value response")

        data = {self.nodename: {"gpus": self.parse_response(response)}}
        return self.transform(data)

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
                try:
                    gpu_data[self.query_properties[idx]] = value
                except IndexError:
                    logger.warning(f"Index {idx} for {field} does not exist")
                    raise

            gpus.append(gpu_data)
        return gpus

    def transform(self, data: list[dict, any]) -> list[GPUStatus]:
        samples = []
        timestamp = utcnow()

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
                memory_total=int(value["memory.used"]) + int(value["memory.free"]),
                timestamp=timestamp,
            )
            samples.append(sample)
        return samples

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


class CollectorPool(Generic[T], Observer[T]):
    name: str
    _collectors: list[DataCollector[T]]
    db: Database
    _stop: bool = False
    verbose: bool = True

    def __init__(self, db: Database, name: str = ''):
        super().__init__()

        self.name = name
        self._collectors = []
        self.monitor_thread = Thread(target=self._run, args=())
        self.save_thread = Thread(target=self._save, args=())

        self.db = db

    def add_collector(self, collector: DataCollector[T]):
        if collector not in self._collectors:
            collector.add_observer(self)
            self._collectors.append(collector)

    def _run(self):
        [x.start() for x in self._collectors]
        if self.verbose:
            while not self._stop:
                print(f"{self.name} queue size: {self._samples.qsize()} {utcnow()} (UTC)\r", end="")
                time.sleep(5)
        [x.thread.join() for x in self._collectors]

    def save(self, samples):
        self.db.insert_or_update(samples)

    def _save(self, batch_size: int = 500):
        while not self._stop:
            samples = []
            try:
                for i in range(0, batch_size):
                    sample: T = self._samples.get(block=False)
                    samples.append(sample)
            except Empty:
                pass

            try:
                if samples:
                    self.save(samples)
            except Exception as e:
                logger.warning(f"Error on save -- {e}")

        # Finalize saving of sample after stop
        samples = []
        while True:
            try:
                samples.append(self._samples.get(block=False))
            except Empty:
                if samples:
                    self.save(samples)
                break
        assert self._samples.qsize() == 0
    def start(self, verbose: bool = True):
        self._stop = False
        self.verbose = verbose

        self.monitor_thread.start()
        self.save_thread.start()

    def stop(self):
        self._stop = True
        [x.stop() for x in self._collectors]

    def join(self):
        self.monitor_thread.join()
        self.save_thread.join()


class NvidiaInfoCollector(GPUStatusCollector):
    def __init__(self, nodename: str, user: str = None, sampling_interval_in_s: int | None = None):
        super().__init__(nodename=nodename, gpu_type="nvidia", user=user, sampling_interval_in_s=sampling_interval_in_s)

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


class HabanaInfoCollector(GPUStatusCollector):
    def __init__(self, nodename: str, user: str = None, sampling_interval_in_s: int | None = None):
        super().__init__(nodename=nodename, gpu_type="habana", user=user, sampling_interval_in_s=sampling_interval_in_s)

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

    def transform(self, data) -> list[GPUStatus]:
        samples = []
        timestamp = utcnow()
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
                memory_total=int(value["memory.total"]),
                timestamp=timestamp,
            )
            samples.append(sample)

        return samples


class ROCMInfoCollector(GPUStatusCollector):
    def __init__(self, nodename: str, user: str = None, sampling_interval_in_s: int | None = None):
        super().__init__(nodename=nodename, gpu_type="amd", user=user, sampling_interval_in_s=sampling_interval_in_s)

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
        return "--showuniqueid --showproductname --showuse --showmemuse \
                --showmeminfo vram --showvoltage --showtemp --showpower --csv"

    @property
    def query_properties(self):
        return [
            "device",
            "Unique ID",  # uuid
            "Temperature (Sensor edge) (C)",  # 'temperature.sensor_edge
            "Temperature (Sensor junction) (C)",  # temperature.sensor_junction
            "Temperature (Sensor memory) (C)",  # temperature.sensor_memory
            # optional
            "Temperature (Sensor HBM 0) (C)",  # temperature.sensor_hbm_0
            "Temperature (Sensor HBM 1) (C)",  # temperature.sensor_hbm_1
            "Temperature (Sensor HBM 2) (C)",  # temperature.sensor_hbm_2
            "Temperature (Sensor HBM 3) (C)",  # temperature.sensor_hbm_3
            "Average Graphics Package Power (W)",  # power.draw
            "GPU use (%)",  # utilization.gpu
            "GFX Activity",  # utilization.gfx,
            "GPU memory use (%)",  # utilization.memory / high or low (1 or 0)
            "Memory Activity",
            "Voltage (mV)",
            "VRAM Total Memory (B)",  # memory.total
            "VRAM Total Used Memory (B)",  # memory used
            "Card series",
            "Card model",
            "Card vendor",
            "Card SKU"
        ]

    def send_request(self, nodename: str, user: str) -> str:
        msg = f"ssh {user}@{nodename} '{self.query_cmd} {self.query_argument}'"
        return subprocess.run(msg, shell=True, stdout=subprocess.PIPE).stdout.decode(
            "utf-8"
        )

    def parse_response(self, response: str) -> dict[str]:
        gpus = []
        main_response = [x for x in response.strip().split("\n") if not x.lower().startswith("warn")]

        field_names = main_response[0].split(",")
        for line in main_response[1:]:
            gpu_data = {}
            for idx, field in enumerate(line.split(",")):
                property_name = field_names[idx]
                if property_name not in self.query_properties:
                    continue

                value = field.strip()
                gpu_data[property_name] = value
            gpus.append(gpu_data)
        return gpus

    def transform(self, data) -> list[GPUStatus]:
        samples = []
        timestamp = utcnow()

        for idx, value in enumerate(data[self.nodename]["gpus"]):
            sample = GPUStatus(
                name=value["Card series"],
                uuid=value["Unique ID"],
                local_id=idx,
                node=self.nodename,
                power_draw=value["Average Graphics Package Power (W)"],
                temperature_gpu=value["Temperature (Sensor edge) (C)"],
                utilization_memory=int(value["VRAM Total Used Memory (B)"])
                * 1.0
                / int(value["VRAM Total Memory (B)"]),
                utilization_gpu=value["GPU use (%)"],
                memory_total=int(value["VRAM Total Memory (B)"])/(1024.0**2), # Use MB
                timestamp=timestamp,
            )
            samples.append(sample)
        return samples

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


class GPUStatusCollectorPool(CollectorPool[GPUStatus]):
    def add_collector(self, collector: DataCollector[T]):
        super().add_collector(collector)
        self.db.insert_or_update(Nodes(name=collector.nodename))

    def save(self, samples):
        self.db.insert(samples)


class JobStatusCollector(DataCollector[JobStatus], Observable[JobStatus]):
    user: str = None

    def __init__(self, user: str = None):
        super().__init__(name="job-info-collector", sampling_interval_in_s=30)
        self.observers = []

        if user is None:
            self.user = self.get_user()

    def run(self) -> list[JobStatus]:
        response = Slurm.get_slurmrestd("/jobs")

        if response == "" or response is None:
            raise ValueError("JobsCollector: No value response")

        return [JobStatus.from_json(x) for x in response["jobs"]]

def cli_run():
    Slurm.ensure_restd()

    parser = ArgumentParser()
    parser.add_argument("mode", default="prod")

    args, unknown = parser.parse_known_args()

    db_settings = DatabaseSettings()
    db_home = Path(db_settings.uri.replace("sqlite:///","")).parent
    db_home.mkdir(parents=True, exist_ok=True)

    if args.mode == "dev":
        db_settings.uri = db_settings.uri.replace(".sqlite", ".dev.sqlite")
        logger.warning(f"Running in development mode: using {db_settings.uri}")
    elif args.mode == "prod":
        logger.warning(f"Running in production mode: using {db_settings.uri}")
    else:
        logger.warning("Missing 'mode'")
        print(parser)
        sys.exit(10)

    run(db_settings)

def run(db_settings: DatabaseSettings | None = None):
    if db_settings is None:
        db_settings = DatabaseSettings()

    db = SlurmMonitorDB(db_settings=db_settings)
    gpu_pool = GPUStatusCollectorPool(db=db, name='gpu status')

    collectors = [
        # Nvidia Volta V100
        NvidiaInfoCollector(nodename="g001"),
        # Nvidia Volta A100
        NvidiaInfoCollector(nodename="g002"),
        # AMD Vega20
        ROCMInfoCollector(nodename="n001"),
        ROCMInfoCollector(nodename="n002"),
        ROCMInfoCollector(nodename="n003"),
        # AMD Instinct Mi100
        ROCMInfoCollector(nodename="n004"),
        NvidiaInfoCollector(nodename="n009"),
        NvidiaInfoCollector(nodename="n010"),
        NvidiaInfoCollector(nodename="n011"),
        NvidiaInfoCollector(nodename="n012"),
        NvidiaInfoCollector(nodename="n013"),
        NvidiaInfoCollector(nodename="n014"),
        ROCMInfoCollector(nodename="n015"),
        ROCMInfoCollector(nodename="n016"),
        # Habana HL205
        HabanaInfoCollector(nodename="h001"),
    ]

    [gpu_pool.add_collector(x) for x in collectors]
    gpu_pool.start()

    job_status_collector = JobStatusCollector()
    jobs_pool = CollectorPool[JobStatus](db=db, name='job status')
    jobs_pool.add_collector(job_status_collector)
    jobs_pool.start()

    while True:
        answer = input("\nEnter 'q' to quit\n\n")
        if answer.lower().startswith('q'):
            break

    gpu_pool.stop()
    jobs_pool.stop()

if __name__ == "__main__":
    cli_run()
