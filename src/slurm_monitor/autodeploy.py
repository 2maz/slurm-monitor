from __future__ import annotations

import asyncio
import curses
from threading import Thread
import time
import traceback as tb
import datetime as dt
import logging
import json
from pathlib import Path
from pydantic import BaseModel
from pydantic_settings import BaseSettings
import sys

from slurm_monitor.db_operations import DBManager
from slurm_monitor.app_settings import AppSettings
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
    _last_update: dt.datetime | None = None
    _stop: bool = False
    _sampling_interval_in_s: float

    # collect the times when a redeployment took place
    stats: AutoDeployerStats
    stats_filename: str | Path

    app_settings: AppSettings
    cluster_name: str
    deploy_command: str

    _getch_supported: bool

    def __init__(self,
            app_settings: AppSettings | None = None,
            sampling_interval_in_s: float = 5*60,
            stats_filename: str | Path = SLURM_MONITOR_AUTODEPLOYER_JSON,
            cluster_name: str | None = None,
            deploy_command: str | None = None,
            allow_list: list[str] | None = None
        ):
        self.app_settings = app_settings
        self.dbi = DBManager.get_database(app_settings=app_settings)

        self.thread = Thread(target=self.run, args=())
        self._sampling_interval_in_s = sampling_interval_in_s

        self.stats = AutoDeployerStats(nodes={})
        self.stats_filename = stats_filename

        self.cluster_name = cluster_name
        self.deploy_command = deploy_command
        self.allow_list = allow_list

        self.messages = []

        self._screen = None
        self._getch_supported = True


    def start(self):
        self._stop = False
        self.thread.start()

    def stop(self):
        self._stop = True
        self.thread.join()

    async def is_drained(self, node: str) -> bool:
        response = Command.run(f"sinfo -n {node} -N -h -o '%t'")
        return response.startswith("drain")

    def all_nodes(self) -> list[str]:
        response = Command.run("sinfo -N -h -o '%n'")
        return response.splitlines()

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

    def addstr(self, y, x, text, attr = None):
        screenheight, screenwidth = self._screen.getmaxyx()
        if y >= screenheight:
            return

        writeable_x = screenwidth - x -1
        if writeable_x < 1:
            return

        if attr:
            self._screen.addstr(y, x, text[:writeable_x], attr)
        else:
            self._screen.addstr(y, x, text[:writeable_x])

    def show(self):
        try:
            if not self._screen:
                self._screen = curses.initscr()
                self._screen.clear()
                curses.noecho()
                try:
                    curses.cbreak()
                except Exception:
                    logger.warning("Terminal does not support 'cbreak' - key interactions will be disabled")
                    self._getch_supported = False

                self._screen.nodelay(True)

            self._screen.erase()


            elapsed = (utcnow() - self._last_update).total_seconds()
            delta_in_s = self._sampling_interval_in_s - elapsed

            # header
            screenheight, screenwidth = self._screen.getmaxyx()
            current_time = f"-- CURRENT TIME  {utcnow().isoformat(timespec='seconds')} | NEXT UPDATE IN {int(delta_in_s)}s"
            self.addstr(0, 0, f"{current_time}{'-'*(screenwidth-len(current_time))}")
            self.addstr(1, 0, f">> Status: slurm-monitor auto-deploy --cluster-name {self.cluster_name}")
            self.addstr(2, 0, "   q to quit ")
            self.addstr(3, 0, " "*screenwidth)

            for idx, msg in enumerate(self.messages):
                self.addstr(idx + 4, 0, msg)

            if self._getch_supported:
                key = self._screen.getch()
                if key == ord('q'):
                    self._stop = True
                    self.addstr(0,0, "Received user's request to stop ... ]")


            self._screen.refresh()
        except Exception as e:
            logger.error(f"Screen update failed: {e}")
            print(f"Screen update failed: {e}")
            tb.print_tb(e.__traceback__)
            sys.exit(0)

    def run(self):
        self._last_update = None
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            while not self._stop:
                now = utcnow()
                if self._last_update:
                    elapsed = (now - self._last_update).total_seconds()
                    if elapsed < self._sampling_interval_in_s:
                        self.show()
                        time.sleep(1)
                        continue
                else:
                    self._last_update = utcnow()

                now = utcnow()
                self.messages = []
                self.messages.append(f"-- autodeploy check: {now}")

                last_probe_timestamp = None
                if self.app_settings.db_schema_version == "v1":
                    last_probe_timestamp = loop.run_until_complete(self.dbi.get_last_probe_timestamp())
                else:
                    if self.cluster_name is None:
                        raise ValueError("Missing cluster_name")

                    last_probe_timestamp = loop.run_until_complete(
                            self.dbi.get_last_probe_timestamp(cluster=self.cluster_name)
                    )
                    logger.info(last_probe_timestamp)

                expected_nodes = self.all_nodes()
                for node in expected_nodes:
                    node_time = last_probe_timestamp.get(node, None)
                    if not node_time:
                        msg = f"{node} has not been seen before"
                        if self.allow_list is None or node in self.allow_list:
                            msg += ", but is in the allow list"
                            if not loop.run_until_complete(self.is_drained(node)):
                                self.deploy(node)
                            else:
                                msg += ", but is drained"
                        else:
                            msg += ", but is not on the allow list"
                        self.messages.append(msg)
                        continue

                    node_time = node_time.replace(tzinfo=dt.timezone.utc)
                    if node not in self.stats.nodes:
                        self.stats.nodes[node] = AutoDeployerNodeStats(last_seen=node_time)
                    else:
                        self.stats.nodes[node].last_seen = node_time

                    last_seen_in_s = (now - node_time).total_seconds()
                    msg = f"{node} last seen: {last_seen_in_s:10.1f} s ago"
                    if self.allow_list is None or node in self.allow_list:
                        if last_seen_in_s > self._sampling_interval_in_s:
                            if not loop.run_until_complete(self.is_drained(node)):
                                msg = f"{msg} -- requires redeployment of probe"
                                self.deploy(node)
                            else:
                                msg = f"{msg} -- but node is drained"
                    self.messages.append(msg)

                self.save_stats()
                self._last_update = utcnow()
                self.show()
        finally:
            if self._screen:
                self._screen.clear()
                curses.echo()
                curses.nocbreak()
                curses.endwin()

class AutoDeployerSonar(AutoDeployer):
    async def is_drained(self, node: str) -> bool:
        node_states = await self.dbi.get_nodes_states(
            cluster=self.cluster_name,
            nodelist=node,
        )
        if not node_states:
            return False

        for state in node_states[0]['states']:
            if state.lower().startswith("drain"):
                return True

        return False


    def deploy(self, node: str) -> str:
        logger.info(f"Deploying with deploy_command={self.deploy_command}")
        response = Command.run(f"{self.deploy_command} {node}")
        logger.info(response)

        if node not in self.stats.nodes:
            logger.warning(f"Node '{node}' has not been registered in stats yet")
            self.stats.nodes[node] = AutoDeployerNodeStats(last_seen=utcnow())

        self.stats.nodes[node].deploy.append(utcnow())
