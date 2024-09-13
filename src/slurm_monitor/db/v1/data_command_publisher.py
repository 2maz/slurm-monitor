from faststream import FastStream
from faststream.kafka import KafkaBroker
import anyio



from typing import TypeVar

import logging


logger = logging.getLogger(__name__)

T = TypeVar("T")

broker = KafkaBroker("srl-login3.cm.cluster:10092")
app = FastStream(broker)
control_publisher = broker.publisher("slurm-monitor-probe-control")
count = 1

async def publish():
    global count
    while True:
        #message = {"action": "stop", "node": "g002"}
        message = {"action": "set_interval", "interval_in_s": 30}
        await broker.connect()
        await control_publisher.publish(message)
        print(f"send {count}")
        count +=1
        await anyio.sleep(delay=10)

if __name__ == "__main__":
    # Use asyncio.run to start the event loop and run the main coroutine
    anyio.run(publish)
