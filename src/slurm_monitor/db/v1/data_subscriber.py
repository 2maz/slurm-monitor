from argparse import ArgumentParser
from kafka import KafkaConsumer
import time
import logging
import json
import datetime as dt

from slurm_monitor.db.v1.db import SlurmMonitorDB
from slurm_monitor.db.v1.db_tables import (
        CPUStatus,
        GPUs,
        GPUStatus,
        JobStatus,
        MemoryStatus,
        Nodes,
        ProcessStatus
)
from slurm_monitor.db.v1.data_publisher import KAFKA_NODE_STATUS_TOPIC
from slurm_monitor.app_settings import AppSettings

logger = logging.getLogger(__name__)


class MessageHandler:
    database: SlurmMonitorDB
    nodes: dict[str, any]

    def __init__(self, database: SlurmMonitorDB):
        self.database = database
        self.nodes = {}

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
        if x:= sample["memory"]:
            if timestamp is None and "timestamp" in x:
                timestamp = dt.datetime.fromisoformat(x["timestamp"])

            memory_status = MemoryStatus(
                node=nodename,
                timestamp=timestamp,
                **x,
            )

        if nodename not in self.nodes:
            nodes_update[nodename] = Nodes(name=nodename,
                    cpu_count=len(cpu_samples),
                    cpu_model=cpu_model,
                    memory_total=memory_status.total,
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
                     memory_total=x['memory_total']
                )

        if "jobs" in sample:
            for job_id, processes in sample["jobs"].items():
                job = None
                jobs = self.database.fetch_all(JobStatus, JobStatus.job_id == job_id)
                if not jobs:
                    logger.warning(f"Slurm Job {job_id} is not registered yet -- skipping recording")
                    continue
                elif len(jobs) > 1:
                    sorted(jobs, key=lambda x: x.submit_time, reverse=True)

                job = jobs[0]

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
                    processes_status.append(status)

                    self.database.insert(processes_status)

        if gpus:
            self.database.insert_or_update([x for x in gpus.values()])
            self.database.insert(gpu_samples)

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

def cli_run():
    logging.basicConfig()

    parser = ArgumentParser()
    parser.add_argument("--host", type=str, default=None, required=True)
    parser.add_argument("--db-uri", type=str, default=None, help="sqlite:////tmp/sqlite.db or timescaledb://slurmuser:test@localhost:10100/ex3cluster")
    parser.add_argument("--port", type=int, default=10092)
    parser.add_argument("--log-level", type=str, default="INFO", help="Set the logging level")

    parser.add_argument("--topic",
            type=str,
            default=KAFKA_NODE_STATUS_TOPIC,
            help=f"Topic which is subscribed -- default {KAFKA_NODE_STATUS_TOPIC}"
    )

    args, options = parser.parse_known_args()

    logger.setLevel(logging.getLevelName(args.log_level))

    app_settings = AppSettings.initialize()

    if args.db_uri is not None:
        app_settings.database.uri = args.db_uri

    database = SlurmMonitorDB(db_settings=app_settings.database)

    # Use asyncio.run to start the event loop and run the main coroutine
    main(host=args.host,
        port=args.port,
        database=database,
        topic=args.topic)
