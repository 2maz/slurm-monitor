from __future__ import annotations

import asyncio
from threading import Thread
import time
import datetime as dt
import logging
import json
import os
from pathlib import Path
from pydantic import BaseModel
from pydantic_settings import BaseSettings

import slurm_monitor.db_operations as db_ops
from slurm_monitor.app_settings import AppSettings
from slurm_monitor.db.v1.db import SlurmMonitorDB
from slurm_monitor.utils import utcnow
from slurm_monitor.utils.command import Command

logger = logging.getLogger(__name__)

SLURM_MONITOR_AUTODEPLOYER_JSON : str = "slurm-monitor.autodeployer.json"

class AutoDeployerNodeStats(BaseModel):
    last_seen: dt.datetime | None = None
    deploy: list[dt.datetime] = []

class AutoDeployerStats(BaseSettings):
    nodes: dict[str, AutoDeployerNodeStats]

class AutoDeployer:
    thread: Thread
    _stop: bool = False
    _sampling_interval_in_s: float

    # collect the times when a redeployment took place
    stats: AutoDeployerStats
    stats_filename: str | Path

    dbi: SlurmMonitorDB

    def __init__(self,
            app_settings: AppSettings | None = None,
            sampling_interval_in_s: float = 5*60,
            stats_filename: str | Path = SLURM_MONITOR_AUTODEPLOYER_JSON):
        self.dbi = db_ops.get_database(app_settings=app_settings)
        self.thread = Thread(target=self.run, args=())
        self._sampling_interval_in_s = sampling_interval_in_s

        self.stats = AutoDeployerStats(nodes={})
        self.stats_filename = stats_filename

    def start(self):
        self._stop = False
        self.thread.start()

    def stop(self):
        self._stop = True
        self.thread.join()

    def is_drained(self, node: str) -> bool:
        response = Command.run(f"sinfo -n {node} -N -h -o '%t'")
        return response.startswith("drain")

    def deploy(self, node: str) -> str:
        response = Command.run(f"slurm-monitor-probes-ctl -n {node} deploy")
        logger.info(response)

        if node not in self.stats.nodes:
            raise ValueError(f"Node '{node}' has not been registered in stats yet")

        self.stats.nodes[node].deploy.append(utcnow())

    def save_stats(self, filename: str | Path | None = None):
        if filename is None:
            filename = self.stats_filename

        with open(Path(filename), "w") as f:
            json.dump(self.stats.model_dump(), f, indent=4, default=str)

    def run(self):
        start_time = None
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        while not self._stop:
            now = utcnow()
            if start_time:
                elapsed = (now - start_time).total_seconds()
                if elapsed < self._sampling_interval_in_s:
                    time.sleep(self._sampling_interval_in_s - elapsed)
            else:
                start_time = utcnow()

            now = utcnow()
            os.system("clear")
            print(f"-- autodeploy check: {now}")
            last_probe_timestamp = loop.run_until_complete(self.dbi.get_last_probe_timestamp())
            for node in sorted(last_probe_timestamp.keys()):
                node_time = last_probe_timestamp[node].replace(tzinfo=dt.timezone.utc)
                if node not in self.stats.nodes:
                    self.stats.nodes[node] = AutoDeployerNodeStats(last_seen=node_time)
                else:
                    self.stats.nodes[node].last_seen = node_time

                last_seen_in_s = (now - node_time).total_seconds()
                msg = f"{node} last seen: {last_seen_in_s:10.1f} s ago"
                if last_seen_in_s > self._sampling_interval_in_s:
                    if not self.is_drained(node):
                        print(f"{msg} -- requires redeployment of probe")
                        self.deploy(node)
                    else:
                        print(f"{msg} -- but node is drained")
                else:
                    print(msg)

            self.save_stats()
            start_time = utcnow()
