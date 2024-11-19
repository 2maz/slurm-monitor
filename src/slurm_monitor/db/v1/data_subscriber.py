from kafka import KafkaConsumer
import time
import logging
import json
import datetime as dt

from slurm_monitor.db.v1.db import SlurmMonitorDB
from slurm_monitor.db.v1.db_tables import (
        CPUStatus,
        GPUs,
        GPUProcess,
        GPUProcessStatus,
        GPUStatus,
        JobStatus,
        MemoryStatus,
        Nodes,
        ProcessStatus
)
from slurm_monitor.db.v1.data_publisher import KAFKA_NODE_STATUS_TOPIC

logger = logging.getLogger(__name__)


class MessageHandler:
    database: SlurmMonitorDB
    nodes: dict[str, any]

    _unknown_jobs: set[int]

    def __init__(self, database: SlurmMonitorDB):
        self.database = database
        self.nodes = {}

        self._unknown_jobs = set()

    def process(self, message) -> dt.datetime:
        nodes_update = {}

        sample = {}
        try:
            sample = json.loads(message)
            if type(sample) is not dict:
                logger.warning(f"Ignoring invalid message: {message}")
                return
        except Exception as e:
            logger.warning(e)
            return

        nodename = sample["node"]

        timestamp = None
        if "timestamp" in sample:
            timestamp = dt.datetime.fromisoformat(sample['timestamp'])

        cpu_samples = []
        cpu_model = ""
        for x in sample["cpus"]:
            if timestamp is None and "timestamp" in x:
                timestamp = dt.datetime.fromisoformat(x["timestamp"])

            cpu_samples.append(
                    CPUStatus(
                        node=nodename,
                        local_id=x['local_id'],
                        cpu_percent=x['cpu_percent'],
                        timestamp=timestamp,
                    )
            )
            if 'cpu_model' in x:
                cpu_model = x['cpu_model']

        memory_status = None
        memory_total = 0
        if "memory" in sample:
            x = sample["memory"]
            if timestamp is None and "timestamp" in x:
                timestamp = dt.datetime.fromisoformat(x["timestamp"])

            memory_status = MemoryStatus(
                node=nodename,
                timestamp=timestamp,
                **x,
            )
            memory_total = memory_status.total
        else:
            logger.warning(f"No memory status received from {nodename} {' '*80}")

        if nodename not in self.nodes:
            nodes_update[nodename] = Nodes(name=nodename,
                    cpu_count=len(cpu_samples),
                    cpu_model=cpu_model,
                    memory_total=memory_total
            )

        if nodes_update:
            self.database.insert_or_update([x for x in nodes_update.values()])
            self.nodes |= nodes_update

        self.database.insert(cpu_samples)
        if memory_status:
            self.database.insert(memory_status)


        gpus = {}
        gpu_samples = []
        if "gpus" in sample:
            for x in sample["gpus"]:
                uuid = x['uuid']
                gpu_status = GPUStatus(
                    uuid=uuid,
                    temperature_gpu=x['temperature_gpu'],
                    power_draw=x['power_draw'],
                    utilization_gpu=x['utilization_gpu'],
                    utilization_memory=x['utilization_memory'],
                    pstate=x['pstate'],
                    timestamp=dt.datetime.fromisoformat(x['timestamp'])
                )
                gpu_samples.append(gpu_status)

                gpus[uuid] = GPUs(uuid=uuid,
                     node=x['node'],
                     model=x['model'],
                     local_id=x['local_id'],
                     memory_total=x['memory_total'] # in bytes
                )

            self.database.insert_or_update([x for x in gpus.values()])
            self.database.insert(gpu_samples)


        # map process_id to job_id
        process2job = {}
        if "jobs" in sample:
            for job_id, processes in sample["jobs"].items():
                job = None
                jobs = self.database.fetch_all(JobStatus, JobStatus.job_id == job_id)
                if not jobs:
                    if job_id not in self._unknown_jobs:
                        logger.warning(f"Slurm Job {job_id} is not registered yet -- skipping recording")
                        self._unknown_jobs.add(job_id)
                    continue
                elif len(jobs) > 1:
                    sorted(jobs, key=lambda x: x.submit_time, reverse=True)

                job = jobs[0]
                if job_id in self._unknown_jobs:
                    self._unknown_jobs.remove(job_id)

                processes_status = []
                for process in processes:
                    status = ProcessStatus(
                            node=nodename,
                            job_id=job.job_id,
                            job_submit_time=job.submit_time,
                            pid=process['pid'],
                            cpu_percent=process['cpu_percent'],
                            memory_percent=process['memory_percent'],
                            timestamp=timestamp
                    )
                    process2job[status.pid] = job.job_id, job.submit_time

                    processes_status.append(status)

                    self.database.insert(processes_status)

        if "gpu_processes" in sample:
            gpu_processes_status = []
            for gpu_process_stats in sample["gpu_processes"]:
                try:
                    pid = gpu_process_stats["pid"]
                    results = self.database.fetch_all(GPUProcess, where=(GPUProcess.pid == pid))
                    if not results:
                        job_id, job_submit_time = process2job.get(pid, (None, None))
                        gpu_process = GPUProcess(
                            pid=pid,
                            process_name=gpu_process_stats["process_name"],
                            start_time=timestamp,
                            job_id=job_id,
                            job_submit_time=job_submit_time
                        )
                        self.database.insert(gpu_process)
                    gpu_process_status = GPUProcessStatus(
                        uuid=gpu_process_stats['uuid'],
                        pid=pid,
                        utilization_sm=gpu_process_stats['utilization_sm'], # in percent
                        used_memory=gpu_process_stats['used_memory'], # in bytes
                        timestamp=timestamp
                    )
                except Exception as e:
                    logger.warning(e)

                gpu_processes_status.append(gpu_process_status)
            self.database.insert(gpu_processes_status)

        # Current timestamp
        return cpu_samples[0].node, timestamp


def main(*,
        host: str, port: int,
        database: SlurmMonitorDB,
        topic: str = KAFKA_NODE_STATUS_TOPIC,
        retry_timeout_in_s: int = 5,
        ):

    msg_handler = MessageHandler(database=database)
    while True:
        try:
            consumer = KafkaConsumer(topic, bootstrap_servers=f"{host}:{port}")
            start_time = dt.datetime.utcnow()
            while True:
                for idx, msg in enumerate(consumer, 1):
                    try:
                        node, timestamp = msg_handler.process(msg.value.decode("UTF-8"))
                        print(
                                f"{dt.datetime.utcnow()} messages consumed: {idx} since {start_time}"
                                f"-- last received at {timestamp} from {node}      \r", flush=True, end='')
                    except Exception as e:
                        logger.warning(f"Message processing failed: {e}")

        except TimeoutError:
            raise
        except Exception as e:
            logger.warning(f"Connection failed - retrying in 5s - {e}")
            time.sleep(retry_timeout_in_s)

    logger.info("All tasks gracefully stopped")
