from __future__ import annotations

from threading import Thread
import time
import datetime as dt
import logging

import slurm_monitor.db_operations as db_ops
from slurm_monitor.app_settings import AppSettings
from slurm_monitor.db.v1.db import SlurmMonitorDB
from slurm_monitor.utils import utcnow
from slurm_monitor.utils.command import Command

logger = logging.getLogger(__name__)

class AutoDeployer:
    thread: Thread
    _stop: bool = False
    _sampling_interval_in_s: float

    dbi: SlurmMonitorDB

    def __init__(self, app_settings: AppSettings | None = None, sampling_interval_in_s: float = 5*60):
        self.dbi = db_ops.get_database(app_settings=app_settings)
        self.thread = Thread(target=self.run, args=())
        self._sampling_interval_in_s = sampling_interval_in_s

    def start(self):
        self._stop = False
        self.thread.start()

    def stop(self):
        self._stop = False
        self.thread.join()

    def is_drained(self, node: str) -> bool:
        response = Command.run(f"sinfo -n {node} -N -h -o '%t'")
        return response.startswith("drain")

    def deploy(self, node: str) -> str:
        response = Command.run(f"slurm-monitor-probes-ctl -n {node} deploy")
        logger.info(response)

    def run(self):
        start_time = utcnow()
        while not self._stop:
            now = utcnow()
            elapsed = (now - start_time).total_seconds()
            if elapsed < self._sampling_interval_in_s:
                time.sleep(self._sampling_interval_in_s - elapsed)

            now = utcnow()
            print(f"-- autodeploy check: {now}")
            last_probe_timestamp = self.dbi.get_last_probe_timestamp()
            for node in sorted(last_probe_timestamp.keys()):
                node_time = last_probe_timestamp[node].replace(tzinfo=dt.timezone.utc)
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

            start_time = utcnow()
