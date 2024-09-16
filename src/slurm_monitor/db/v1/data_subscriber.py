from argparse import ArgumentParser
from kafka import KafkaConsumer
import time
import logging
import json
import datetime as dt

from .db import DatabaseSettings, SlurmMonitorDB
from .db_tables import CPUStatus, GPUs, GPUStatus, Nodes
from .data_publisher import KAFKA_NODE_STATUS_TOPIC

logger = logging.getLogger(__name__)

nodes = {}
database = None

def handle_message(message):
    global nodes
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
    cpu_samples = [CPUStatus(**(x | {'node': nodename})) for x in sample["cpus"]]

    if nodename not in nodes:
        nodes_update[nodename] = Nodes(name=nodename,
                cpu_count=len(cpu_samples),
                cpu_model=""
        )

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
                timestamp=x['timestamp']
            )
            gpu_samples.append(gpu_status)

            gpus[uuid] = GPUs(uuid=uuid,
                 node=x['node'],
                 model=x['model'],
                 local_id=x['local_id'],
                 memory_total=x['memory_total']
            )

    if nodes_update:
        database.insert_or_update([x for x in nodes_update.values()])
        nodes |= nodes_update

    database.insert_or_update(cpu_samples)
    if gpus:
        database.insert_or_update([x for x in gpus.values()])
        database.insert_or_update(gpu_samples)


def run(*, host: str, port: int, retry_timeout_in_s: int = 5):
    while True:
        try:
            consumer = KafkaConsumer(KAFKA_NODE_STATUS_TOPIC, bootstrap_servers=f"{host}:{port}")
            start_time = dt.datetime.utcnow()
            while True:
                for idx, msg in enumerate(consumer, 1):
                    handle_message(msg.value.decode("UTF-8"))
                    print(f"Messages consumed: {idx} since {start_time}\r", flush=True, end='')
        except Exception as e:
            logger.warning(f"Connection failed - retrying in 5s - {e}")
            time.sleep(retry_timeout_in_s)

    logger.info("All tasks gracefully stopped")

def cli_run():
    global database

    logging.basicConfig()

    parser = ArgumentParser()
    parser.add_argument("--host", type=str, default=None, required=True)
    parser.add_argument("--db-uri", type=str, required=True, help="timescaledb://slurmuser:test@localhost:10100/ex3cluster")
    parser.add_argument("--port", type=int, default=10092)
    parser.add_argument("--log-level", type=str, default="INFO", help="Set the logging level")

    args, options = parser.parse_known_args()

    logger.setLevel(logging.getLevelName(args.log_level))

    db_settings = DatabaseSettings(
        user="", password="", uri=args.db_uri
    )
    database = SlurmMonitorDB(db_settings=db_settings)

    # Use asyncio.run to start the event loop and run the main coroutine
    run(host=args.host, port=args.port)
