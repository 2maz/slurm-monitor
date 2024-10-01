import platform
import psutil
import asyncio
from kafka import KafkaConsumer, KafkaProducer

from argparse import ArgumentParser
import subprocess
import datetime as dt
from abc import abstractmethod
from typing import TypeVar, Callable

import os
import signal
import socket
import json
from pydantic import BaseModel
from pydantic_settings import BaseSettings
import logging
import re

from slurm_monitor.utils import utcnow
from slurm_monitor.utils.process import ProcessStats, JobMonitor
from slurm_monitor.utils.command import Command
from slurm_monitor.utils.system_info import SystemInfo

logger = logging.getLogger(__name__)

KAFKA_NODE_STATUS_TOPIC = "node-status"
KAFKA_PROBE_CONTROL_TOPIC = "slurm-monitor-probe-control"

T = TypeVar("T")

class CPUStatus(BaseModel):
    local_id: int
    cpu_model: str
    cpu_percent: float

class MemoryStatus(BaseModel):
    total: int
    available: int
    percent: float
    used: int
    free: int
    active: int
    inactive: int
    buffers: int
    cached: int
    shared: int
    slab: int

# from .db_tables import GPUs, GPUStatus
class GPUStatus(BaseModel):
    uuid: str
    node: str
    model: str
    local_id: int
    memory_total: int

    temperature_gpu: float
    power_draw: float
    utilization_gpu: float
    utilization_memory: float

    pstate: str | None = None
    timestamp: str | dt.datetime


class NodeStatus(BaseSettings):
    node: str
    cpus: list[CPUStatus]
    gpus: list[GPUStatus]
    memory: MemoryStatus
    jobs: dict[int, list[ProcessStats]]

    timestamp: dt.datetime

class DataCollector():
    sampling_interval_in_s: int
    name: str

    def __init__(self, name: str, sampling_interval_in_s: int):
        self.name = name
        self.sampling_interval_in_s = sampling_interval_in_s

    async def collect(self,
            shutdown_event: asyncio.Event,
            publish_fn: Callable[[NodeStatus], bool],
            max_samples: int | None = None
        ):
        samples_collected = 0
        while not shutdown_event.is_set():
            try:
                sample: NodeStatus = self.get_node_status()
                if publish_fn:
                    publish_fn(sample)

                if max_samples is not None:
                    samples_collected += 1
                    if samples_collected >= max_samples:
                        logger.warning(f"Max number of samples collected ({max_samples}). Stopping")
                        shutdown_event.set()


            except Exception as e:
                shutdown_event.set()
                logger.warning(f"{e.__class__} {e}")
            finally:
                logger.debug(f"Sleeping for {self.sampling_interval_in_s}")
                await asyncio.sleep(self.sampling_interval_in_s)

    def get_node_status(self) -> NodeStatus:
        raise NotImplementedError()

class NodeStatusCollector(DataCollector):
    system_info: SystemInfo

    gpu_type: str | None

    def __init__(self, gpu_type: str | None = None, sampling_interval_in_s: int | None = None):
        if sampling_interval_in_s is None:
            sampling_interval_in_s = 20

        super().__init__(name=f"collector-{platform.node()}", sampling_interval_in_s=sampling_interval_in_s)

        self.nodename = platform.node()
        self.gpu_type = gpu_type

        self.system_info = SystemInfo()

    def has_gpus(self) -> bool:
        return self.gpu_type is not None

    def get_node_status(self) -> NodeStatus:
        gpu_status = []
        if self.has_gpus():
            # TODO: compact again
            response = self.get_gpu_status()
            if response == "" or response is None:
                raise ValueError("NodeStatusCollector: No value response")

            data = {self.nodename: {"gpus": self.parse_response(response)}}
            gpu_status = self.transform(data)

        timestamp = utcnow()
        cpu_status = [
                CPUStatus(
                    cpu_model=self.system_info.cpu_info.get_cpu_model(),
                    cpu_percent=percent,
                    local_id=idx)
                for idx, percent in enumerate(psutil.cpu_percent(percpu=True))
        ]

        memory_status = MemoryStatus(**psutil.virtual_memory()._asdict())

        job_status = JobMonitor.get_active_jobs()
        return NodeStatus(
                node=platform.node(),
                gpus=gpu_status,
                cpus=cpu_status,
                memory=memory_status,
                jobs=job_status.jobs,
                timestamp=timestamp)

    def get_gpu_status(self) -> str:
        msg = f"{self.query_cmd} {self.query_argument}={','.join(self.query_properties)}"
        return Command.run(msg)

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
                model=value["name"],
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


class NvidiaInfoCollector(NodeStatusCollector):
    def __init__(self, sampling_interval_in_s: int | None = None):
        super().__init__(gpu_type="nvidia", sampling_interval_in_s=sampling_interval_in_s)

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


class HabanaInfoCollector(NodeStatusCollector):
    def __init__(self, sampling_interval_in_s: int | None = None):
        super().__init__(gpu_type="habana", sampling_interval_in_s=sampling_interval_in_s)

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
                model=value["name"],
                uuid=value["uuid"],
                local_id=idx,
                node=self.nodename,
                power_draw=value["power.draw"],
                temperature_gpu=value["temperature.aip"],
                utilization_memory=int(value["memory.used"])
                * 100.0
                / int(value["memory.total"]),
                utilization_gpu=value["utilization.aip"],
                memory_total=value["memory.total"],
                timestamp=timestamp,
            )
            samples.append(sample)

        return samples

class XPUInfoCollector(NodeStatusCollector):
    devices: dict[str,any]

    def __init__(self, sampling_interval_in_s: int | None = None):
        super().__init__(gpu_type="xpu", sampling_interval_in_s=sampling_interval_in_s)

        self.discover()

    def discover(self):
        devices_json = Command.run("xpu-smi discovery -j")
        devices_data = json.loads(devices_json)
        self.devices = {}

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
                self.devices[device_id] = json.loads(device_json)

    @property
    def query_cmd(self):
        return "xpu-smi dump"

    def get_gpu_info(self) -> str:
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

    def parse_response(self, response: str) -> dict[str]:
        gpus = []
        for line in response.strip().splitlines()[1:]:
            gpu_data = {}
            for idx, field in enumerate(line.split(',')):
                value = field.strip()
                try:
                    gpu_data[self.query_properties[idx]] = value
                except IndexError:
                    logger.warning(f"Index {idx} for {field} does not exist")
                    raise
            gpus.append(gpu_data)
        return gpus

    def transform(self, data) -> list[GPUStatus]:
        samples = []
        timestamp = utcnow()
        for idx, value in enumerate(data[self.nodename]["gpus"]):
            device_data = self.devices[idx]
            try:
                core_temperature = float(value["GPU Core Temperature (Celsius Degree)"])
            except ValueError:
                core_temperature = 0

            sample = GPUStatus(
                model=device_data["device_name"],
                uuid=device_data["uuid"],
                local_id=idx,
                node=self.nodename,
                power_draw=float(value["GPU Power (W)"]),
                temperature_gpu=float(core_temperature),
                utilization_memory=float(value["GPU Memory Utilization (%)"]),
                utilization_gpu=float(value["GPU Utilization (%)"]),
                memory_total=float(device_data["memory_physical_size_byte"])/(1024.0**2), # MB
                timestamp=timestamp,
            )
            samples.append(sample)

        return samples


class ROCMInfoCollector(NodeStatusCollector):
    def __init__(self, sampling_interval_in_s: int | None = None):
        super().__init__(gpu_type="amd", sampling_interval_in_s=sampling_interval_in_s)

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

    def get_gpu_status(self) -> str:
        return Command.run(f"{self.query_cmd} {self.query_argument}")

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
                model=value["Card series"],
                uuid=value["Unique ID"],
                local_id=idx,
                node=self.nodename,
                power_draw=value["Average Graphics Package Power (W)"],
                temperature_gpu=value["Temperature (Sensor edge) (C)"],
                utilization_memory=int(value["GPU memory use (%)"]),
                utilization_gpu=value["GPU use (%)"],
                memory_total=int(value["VRAM Total Memory (B)"])/(1024**2),
                timestamp=timestamp,
            )
            samples.append(sample)
        return samples


def check_command(command: str):
    p = subprocess.run(command, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if p.returncode == 0:
        return True
    return False

def get_status_collector() -> NodeStatusCollector:
    if check_command(command="nvidia-smi -L"):
        return NvidiaInfoCollector()

    if check_command(command="rocm-smi -a"):
        response = Command.run("rocm-smi -i --csv")
        if response:
            return ROCMInfoCollector()

    if check_command(command="hl-smi"):
        return HabanaInfoCollector()

    if check_command(command="xpu-smi"):
        return XPUInfoCollector()

    return NodeStatusCollector()


class Controller:
    collector: DataCollector
    consumer: KafkaConsumer
    shutdown_event: asyncio.Event

    hostname: str

    def __init__(self, collector: DataCollector,
            bootstrap_servers: str,
            shutdown_event: asyncio.Event,
            listen_interval_in_s: int = 2,
            subscriber_topic: str = KAFKA_PROBE_CONTROL_TOPIC):
        self.consumer = KafkaConsumer(KAFKA_PROBE_CONTROL_TOPIC, bootstrap_servers=bootstrap_servers)
        self.collector = collector
        self.shutdown_event = shutdown_event

        self._listen_interval_in_s = listen_interval_in_s
        self.hostname = socket.gethostname()

    async def run(self):
        while not self.shutdown_event.is_set():
            msg = self.consumer.poll()
            if msg:
                command = json.loads(msg.decode("UTF-8"))
                logger.debug(f"Control command received: {command}")
                self.handle(command)

            await asyncio.sleep(self._listen_interval_in_s)

    # Handle control messages
    def handle(self, data):
        handle_message = False
        if "node" in data:
            node = data["node"]
            if type(node) == list:
                for name_pattern in node:
                    if re.match(name_pattern, self.hostname):
                        handle_message = True
                        break
            elif re.match(node, self.hostname):
                handle_message = True

        if not handle_message:
            # this is not the correct receiver
            return

        if "action" in data:
            action = data["action"]
            if action == "stop":
                logger.warning(f"Shutdown requested for {self.hostname}")
                self.shutdown_event.set()
                os.kill(os.getpid(), signal.SIGINT)
            elif action == "set_interval":
                new_interval = int(data["interval_in_s"])
                logger.info(f"Setting new interval: {new_interval}")
                self.collector.sampling_interval_in_s = new_interval


# Main function to run the app and scheduler
async def main(*, host: str, port: int,
        publisher_topic: str = KAFKA_NODE_STATUS_TOPIC,
        subscriber_topic: str = KAFKA_PROBE_CONTROL_TOPIC,
        max_samples: int | None = None):
    shutdown_event = asyncio.Event()

    broker = f"{host}:{port}"
    status_collector = get_status_collector()

    node_status_producer = KafkaProducer(
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            bootstrap_servers=broker
    )

    def publish_fn(sample: NodeStatus):
        try:
            future = node_status_producer.send(publisher_topic, sample.model_dump())
            future.get(timeout=10)
            logger.debug(f"Published message (on topic: {publisher_topic})")
            return True
        except Exception as e:
            logger.warning(f"Publishing failed: {e}")
            return False

    # Schedule the periodic publisher
    collector_task = asyncio.create_task(status_collector.collect(shutdown_event, publish_fn, max_samples))
    controller = Controller(status_collector,
            bootstrap_servers=broker,
            shutdown_event=shutdown_event,
            subscriber_topic=subscriber_topic)
    control_task = asyncio.create_task(controller.run())

    tasks = [control_task, collector_task]
    # Wait for all tasks to complete (they run forever)
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        print("Application shutdown requested")
    finally:
        shutdown_event.set()
        [await x for x in tasks]
        print("All tasks gracefully stopped")

def cli_run():
    parser = ArgumentParser()
    parser.add_argument("--host", type=str, default=None, required=True, help="Kafka broker's hostname")
    parser.add_argument("--port", type=int, default=10092, help="Port on which the kafka broker is listening")
    parser.add_argument("--log-level", type=str, default="INFO", help="Log level to set")
    parser.add_argument("--number", "-n", type=int, default=None,
            help="Number of collected samples after which the probe is stopped")

    parser.add_argument("--publisher-topic",
            type=str,
            default=KAFKA_NODE_STATUS_TOPIC,
            help=f"Topic under which samples are published -- default {KAFKA_NODE_STATUS_TOPIC}"
    )
    parser.add_argument("--subscriber-topic",
            type=str,
            default=KAFKA_PROBE_CONTROL_TOPIC,
            help=f"Topic which is subscribed for control messages -- default {KAFKA_PROBE_CONTROL_TOPIC}"
    )

    args, options = parser.parse_known_args()

    logging.basicConfig(
            format='%(asctime)s %(levelname)-8s %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
    )
    logger.setLevel(logging.getLevelName(args.log_level.upper()))

    # Use asyncio.run to start the event loop and run the main coroutine
    asyncio.run(main(host=args.host, port=args.port,
        publisher_topic=args.publisher_topic,
        subscriber_topic=args.subscriber_topic,
        max_samples=args.number
        ))


if __name__ == "__main__":
    cli_run()
