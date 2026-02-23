import platform
import psutil
import asyncio
from kafka import KafkaConsumer, KafkaProducer

import datetime as dt
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
from slurm_monitor.utils.system_info import SystemInfo

from slurm_monitor.devices.gpu import GPUStatus, GPUProcessStatus

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


class NodeStatus(BaseSettings):
    node: str
    cpus: list[CPUStatus]
    gpus: list[GPUStatus]
    gpu_processes: list[GPUProcessStatus]
    memory: MemoryStatus
    jobs: dict[int, list[ProcessStats]]

    timestamp: dt.datetime


class DataCollector:
    sampling_interval_in_s: int
    name: str

    def __init__(self, name: str, sampling_interval_in_s: int):
        self.name = name
        self.sampling_interval_in_s = sampling_interval_in_s

    async def collect(
        self,
        shutdown_event: asyncio.Event,
        publish_fn: Callable[[NodeStatus], bool],
        max_samples: int | None = None,
    ):
        samples_collected = 0
        while not shutdown_event.is_set():
            try:
                sample: NodeStatus = self.get_node_status()
                if publish_fn:
                    publish_fn(sample)
                else:
                    print(
                        json.dumps(sample.model_dump(), indent=2, default=str),
                        flush=True,
                    )

                if max_samples is not None:
                    samples_collected += 1

                    if samples_collected >= max_samples:
                        logger.warning(
                            f"Max number of samples collected ({max_samples}). Stopping"
                        )
                        shutdown_event.set()

            except Exception as e:
                logger.warning(f"{e.__class__} {e}")
            finally:
                if not shutdown_event.is_set():
                    logger.debug(f"Sleeping for {self.sampling_interval_in_s}")
                    await asyncio.sleep(self.sampling_interval_in_s)

    def get_node_status(self) -> NodeStatus:
        raise NotImplementedError()


class NodeStatusCollector(DataCollector):
    system_info: SystemInfo

    def __init__(self, sampling_interval_in_s: int | None = None):
        if sampling_interval_in_s is None:
            sampling_interval_in_s = 20

        super().__init__(
            name=f"collector-{platform.node()}",
            sampling_interval_in_s=sampling_interval_in_s,
        )

        self.nodename = platform.node()
        self.system_info = SystemInfo()

    def get_node_status(self) -> NodeStatus:
        gpu_status = []
        gpu_processes = []
        if self.system_info._gpu is not None:
            gpu_status = self.system_info._gpu.get_status()
            gpu_processes = self.system_info._gpu.get_processes()

        timestamp = utcnow()
        cpu_model = self.system_info.cpu_info.get_cpu_model()
        cpu_status = [
            CPUStatus(cpu_model=cpu_model, cpu_percent=percent, local_id=idx)
            for idx, percent in enumerate(psutil.cpu_percent(percpu=True))
        ]

        memory_status = MemoryStatus(**psutil.virtual_memory()._asdict())

        job_status = JobMonitor.get_active_jobs()
        return NodeStatus(
            node=platform.node(),
            gpus=gpu_status,
            gpu_processes=gpu_processes,
            cpus=cpu_status,
            memory=memory_status,
            jobs=job_status.jobs,
            timestamp=timestamp,
        )


class Controller:
    collector: DataCollector
    consumer: KafkaConsumer
    shutdown_event: asyncio.Event

    hostname: str

    def __init__(
        self,
        collector: DataCollector,
        bootstrap_servers: str,
        shutdown_event: asyncio.Event,
        listen_interval_in_s: int = 2,
        subscriber_topic: str = KAFKA_PROBE_CONTROL_TOPIC,
    ):
        self.consumer = KafkaConsumer(
            KAFKA_PROBE_CONTROL_TOPIC, bootstrap_servers=bootstrap_servers
        )
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
            if type(node) is list:
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
async def main(
    *,
    host: str,
    port: int,
    publisher_topic: str = KAFKA_NODE_STATUS_TOPIC,
    subscriber_topic: str = KAFKA_PROBE_CONTROL_TOPIC,
    max_samples: int | None = None,
):
    shutdown_event = asyncio.Event()

    broker = f"{host}:{port}"
    status_collector = NodeStatusCollector()
    logger.info(
        f"Running status collector - gpus: {status_collector.system_info.gpu_info}"
    )

    node_status_producer = KafkaProducer(
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        bootstrap_servers=broker,
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
    collector_task = asyncio.create_task(
        status_collector.collect(
            shutdown_event=shutdown_event,
            publish_fn=publish_fn,
            max_samples=max_samples,
        )
    )
    controller = Controller(
        status_collector,
        bootstrap_servers=broker,
        shutdown_event=shutdown_event,
        subscriber_topic=subscriber_topic,
    )
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
