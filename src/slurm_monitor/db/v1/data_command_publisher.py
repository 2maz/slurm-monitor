from faststream import FastStream
from faststream.kafka import KafkaBroker
import aiokafka
import anyio

import platform
import psutil

from dataclasses import dataclass

from argparse import ArgumentParser
import subprocess
import json
from pathlib import Path
import sys
from threading import Thread
import time
import datetime as dt
from abc import abstractmethod
from queue import Queue, Empty
from typing import Generic, TypeVar
import os

import logging
import re

from slurm_monitor.utils import utcnow
from slurm_monitor.db.db import SlurmMonitorDB, DatabaseSettings, Database
from slurm_monitor.db.db_tables import GPUStatus, JobStatus, Nodes

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

