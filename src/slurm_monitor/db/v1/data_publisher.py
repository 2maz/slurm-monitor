import platform
import psutil
import asyncio
from kafka import KafkaConsumer, KafkaProducer


from argparse import ArgumentParser
import subprocess
import datetime as dt
from abc import abstractmethod
from typing import TypeVar

import os
import signal
import socket
import json

import logging
import re

from slurm_monitor.utils import utcnow
from slurm_monitor.utils.process import ProcessStats, JobMonitor

from pydantic import BaseModel
from pydantic_settings import BaseSettings

logger = logging.getLogger(__name__)

KAFKA_NODE_STATUS_TOPIC = "node-status"
KAFKA_PROBE_CONTROL_TOPIC = "slurm-monitor-probe-control"

T = TypeVar("T")

class CPUStatus(BaseModel):
    local_id: int
    cpu_percent: float

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
    jobs: dict[int, list[ProcessStats]]

    timestamp: dt.datetime

class DataCollector():
    _stop: bool = False

    sampling_interval_in_s: int
    name: str

    def __init__(self, name: str, sampling_interval_in_s: int):
        self.name = name
        self.sampling_interval_in_s = sampling_interval_in_s

    async def collect(self, shutdown_event: asyncio.Event, publish_fn):
        while not shutdown_event.is_set() and not self._stop:
            try:
                sample: NodeStatus = self.run()
                if publish_fn:
                    publish_fn(sample)

            except Exception as e:
                self._stop = True
                logger.warning(f"{e.__class__} {e}")
            finally:
                logger.debug(f"Sleeping for {self.sampling_interval_in_s}")
                await asyncio.sleep(self.sampling_interval_in_s)

    def run(self):
        raise NotImplementedError()

class NodeStatusCollector(DataCollector):
    nodename: str
    gpu_type: str | None

    def __init__(self, gpu_type: str | None = None, sampling_interval_in_s: int | None = None):
        if sampling_interval_in_s is None:
            sampling_interval_in_s = 20

        super().__init__(name=f"collector-{platform.node()}", sampling_interval_in_s=sampling_interval_in_s)

        self.nodename = platform.node()
        self.local_id_mapping = {}
        self.gpu_type = gpu_type

    def has_gpus(self) -> bool:
        return self.gpu_type is not None

    def run(self) -> NodeStatus:
        gpu_status = []
        if self.has_gpus():
            response = self.get_gpu_info()
            if response == "" or response is None:
                raise ValueError("NodeStatusCollector: No value response")

            data = {self.nodename: {"gpus": self.parse_response(response)}}
            gpu_status = self.transform(data)

        timestamp = utcnow()
        cpu_status = [
                CPUStatus(cpu_percent=percent, local_id=idx)
                for idx, percent in enumerate(psutil.cpu_percent(percpu=True))
        ]

        job_status = JobMonitor.get_active_jobs()
        return NodeStatus(
                node=platform.node(),
                gpus=gpu_status,
                cpus=cpu_status,
                jobs=job_status.jobs,
                timestamp=timestamp)

    def get_gpu_info(self) -> str:
        msg = f"{self.query_cmd} {self.query_argument}={','.join(self.query_properties)}"
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

    def get_local_id_mapping(self) -> dict[str, int]:
        msg = f"{self.query_cmd} -L"
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

    def get_gpu_info(self) -> str:
        msg = f"{self.query_cmd} {self.query_argument}"
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

    def get_local_id_mapping(self) -> dict[str, int]:
        msg = f"{self.query_cmd} --showuniqueid --csv"
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

def has_command(command: str):
    p = subprocess.run(command, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if p.returncode == 0:
        return True
    return False

def get_status_collector() -> NodeStatusCollector:
    if has_command(command="nvidia-smi -L"):
        return NvidiaInfoCollector()
    elif has_command(command="rocm-smi -a"):
        return ROCMInfoCollector()
    elif has_command(command="hl-smi"):
        return HabanaInfoCollector()
    else:
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
        subscriber_topic: str = KAFKA_PROBE_CONTROL_TOPIC):
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
    collector_task = asyncio.create_task(status_collector.collect(shutdown_event, publish_fn))
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
    parser.add_argument("--host", type=str, default=None, required=True)
    parser.add_argument("--port", type=int, default=10092)
    parser.add_argument("--log-level", type=str, default="INFO")

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
    logger.setLevel(logging.getLevelName(args.log_level))

    # Use asyncio.run to start the event loop and run the main coroutine
    asyncio.run(main(host=args.host, port=args.port,
        publisher_topic=args.publisher_topic,
        subscriber_topic=args.subscriber_topic
        ))


if __name__ == "__main__":
    cli_run()
