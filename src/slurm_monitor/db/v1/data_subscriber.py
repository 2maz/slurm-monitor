from argparse import ArgumentParser
from faststream import FastStream
from faststream.kafka import KafkaBroker
import aiokafka
import asyncio
from .db import DatabaseSettings, SlurmMonitorDB
from .db_tables import CPUStatus, GPUs, GPUStatus, Nodes

broker = KafkaBroker()
app = FastStream(broker)

from .data_publisher import KAFKA_NODE_STATUS_TOPIC

nodes = {}
database = None

@broker.subscriber(KAFKA_NODE_STATUS_TOPIC, batch=True)
def handle_message(samples):
    global nodes
    nodes_update = {}

    for sample in samples:
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
                     model=x['name'],
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

async def main(*, host: str, port: int):
    await broker.connect(f"{host}:{port}")

    # Schedule the FastStream app to run
    app_task = asyncio.create_task(app.run())

    try:
        await asyncio.gather(app_task)
    except asyncio.CancelledError:
        print("Application shutdown requested")
    finally:
        print("All tasks gracefully stopped")

def cli_run():
    global database

    parser = ArgumentParser()
    parser.add_argument("--host", type=str, default=None, required=True)
    parser.add_argument("--db_uri", type=str, required=True, help="timescaledb://slurmuser:test@localhost:7000/ex3cluster")
    parser.add_argument("--port", type=int, default=10092)

    args, options = parser.parse_known_args()

    db_settings = DatabaseSettings(
        user="", password="", uri=args.db_uri
    )
    database = SlurmMonitorDB(db_settings=db_settings)

    # Use asyncio.run to start the event loop and run the main coroutine
    asyncio.run(main(host=args.host, port=args.port))
