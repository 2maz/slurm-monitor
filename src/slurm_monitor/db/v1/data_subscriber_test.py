from argparse import ArgumentParser
from faststream import FastStream
from faststream.kafka import KafkaBroker
import asyncio
#from .data_publisher import KAFKA_NODE_STATUS_TOPIC

broker = KafkaBroker(retry_backoff_ms=1000,connections_max_idle_ms=None)
app = FastStream(broker)

KAFKA_NODE_STATUS_TOPIC = "node-status"
KAFKA_PROBE_CONTROL_TOPIC = "slurm-monitor-probe-control"

nodes = {}

@broker.subscriber(KAFKA_NODE_STATUS_TOPIC, batch=True)
def handle_message(samples):
    print(f"received: {samples}")

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
    parser.add_argument("--port", type=int, default=10092)

    args, options = parser.parse_known_args()

    # Use asyncio.run to start the event loop and run the main coroutine
    asyncio.run(main(host=args.host, port=args.port))

if __name__ == "__main__":
    cli_run()
