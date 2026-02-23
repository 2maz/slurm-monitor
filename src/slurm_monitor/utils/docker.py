from argparse import ArgumentParser
import json
import logging
from pathlib import Path
from io import StringIO
import pandas as pd
from typing import ClassVar
from psutil import Process

from slurm_monitor.utils.command import Command
from slurm_monitor.utils.slurm import Slurm
from slurm_monitor.utils.stats_types import ProcessStats

logger = logging.getLogger(__name__)


class Docker:
    argument_parser: ArgumentParser

    DOCKER_BIN = "docker"
    _BIN_HINTS: ClassVar[list[str]] = [
        "/cm/shared/apps/docker/current/sbin/",
        "/cm/shared/apps/docker/current/bin/",
        "/cm/local/apps/docker/current/sbin/",
        "/cm/local/apps/docker/current/bin/",
    ]
    _ensured_docker: ClassVar[str] = None

    def __init__(self):
        # ensure that the arguments are consumed to identify the relevant ones
        self.argument_parser = ArgumentParser()
        self.argument_parser.add_argument("main", default=None, help="main command")
        self.argument_parser.add_argument("subparser", default=None, help="subparser")

        self.argument_parser.add_argument(
            "-d", "--detach", action="store_true", default=False
        )
        self.argument_parser.add_argument(
            "-e", "--env", action="extend", nargs=1, metavar=("env")
        )
        self.argument_parser.add_argument(
            "-i", "--interactive", action="store_true", default=False
        )
        self.argument_parser.add_argument("--gpus", default=None, help="gpus")
        self.argument_parser.add_argument("--name", default=None)
        self.argument_parser.add_argument("--network", default=None)
        self.argument_parser.add_argument(
            "--privileged", action="store_true", default=False
        )
        self.argument_parser.add_argument("--rm", action="store_true", default=None)
        self.argument_parser.add_argument("--shm-size", default=None)
        self.argument_parser.add_argument(
            "-t", "--tty", action="store_true", default=False
        )
        self.argument_parser.add_argument("-u", "--user", default=None)
        self.argument_parser.add_argument(
            "-v", "--volume", action="extend", nargs=1, metavar=("map")
        )
        self.argument_parser.add_argument("-w", "--workdir", default=None)
        self.argument_parser.add_argument(
            "-p", "--expose", action="extend", nargs=1, metavar=("ports")
        )
        self.argument_parser.add_argument(
            "-P", "--publish-all", action="store_true", default=False
        )

        self.argument_parser.add_argument("image", default=None, help="image")

        try:
            self.ensure_docker()
        except RuntimeError as e:
            logger.warning(f"Could not find docker -- {e}")

    def is_available(self) -> bool:
        return self._ensured_docker

    @classmethod
    def ensure_docker(cls):
        if cls._ensured_docker is None:
            setattr(
                cls, "DOCKER_BIN", Command.find(command="docker", hints=cls._BIN_HINTS)
            )
            cls._ensured_docker = "docker"
        return getattr(cls, "DOCKER_BIN")

    @classmethod
    def inspect(cls) -> dict[str, any]:
        details = {}
        response = Command.run(f"{cls.DOCKER_BIN} ps -q")
        container_ids = response.split("\n")
        for container in container_ids:
            response = Command.run(f"{cls.DOCKER_BIN} inspect {container}")
            details[container] = json.loads(response)[0]

        return details

    @classmethod
    def get_pids(cls) -> list[int]:
        try:
            response = Command.run("pidof docker run")
            if response:
                return [int(x) for x in response.strip().split(" ")]

        except RuntimeError:
            pass
            # no docker run available
        return []

    @classmethod
    def get_commandline(cls, pid: int) -> str:
        return " ".join(Process(pid).cmdline())

    def find_associated_to_job(self, job_id: int) -> dict[str, any]:
        containers = {}
        job_related_pids = Slurm.get_associated_pids(job_id=job_id)
        if not job_related_pids:
            return containers

        docker_pids = Docker.get_pids()
        for pid in job_related_pids:
            if pid in docker_pids:
                associated = self.find_associated(pid)
                if associated:
                    containers.update(associated)

        return containers

    def get_process_stats_by_container(self, container_id: str) -> ProcessStats:
        # see https://docs.docker.com/reference/cli/docker/container/stats/
        response = Command.run(
            f"{self.DOCKER_BIN} stats {container_id} "
            "--no-stream --format "
            "'table "
            "{{.Name}},"
            "{{.CPUPerc}},"
            "{{.MemPerc}},"
            "{{.MemUsage}},"
            "{{.NetIO}},"
            "{{.BlockIO}},"
            "{{.PIDs}}'"
        )
        df = pd.read_csv(StringIO(response.strip()))
        column_names = {x: x.strip() for x in df.columns}
        df.rename(columns=column_names, inplace=True)

        cpu_percent = float(df["CPU %"][0].replace("%", ""))
        mem_percent = float(df["MEM %"][0].replace("%", ""))

        return ProcessStats(pid=0, cpu_percent=cpu_percent, memory_percent=mem_percent)

    def get_process_stats(self, job_id: int) -> list[ProcessStats]:
        stats = []
        containers = self.find_associated_to_job(job_id)
        for container_id, info in containers.items():
            process_stats = self.get_process_stats_by_container(container_id)
            process_stats.pid = int(info["State"]["Pid"])

            stats.append(process_stats)
        return stats

    def find_associated(self, pid: int) -> dict[str, any]:
        cmdline = self.get_commandline(pid=pid)

        args, options = self.argument_parser.parse_known_args(args=cmdline.split(" "))

        if args.subparser != "run":
            raise RuntimeError(
                "Docker.find_associated: this is not a docker run command"
            )

        volumes = []
        for x in args.volume:
            v = x.split(":")
            volumes.append(f"{Path(v[0])}:{Path(v[1])}")

        running_containers = self.inspect()
        for container_id, info in running_containers.items():
            is_match = None
            if info["Config"]["Image"] == args.image:
                is_match = True
            else:
                is_match = False
                break

            for mount in info["Mounts"]:
                source = mount["Source"]
                destination = mount["Destination"]
                expected = f"{source}:{destination}"

                if expected in volumes:
                    is_match = True
                else:
                    is_match = False
                    break

            if is_match:
                return {container_id: running_containers[container_id]}
            return {}


if __name__ == "__main__":
    import sys

    docker = Docker()
    job_id = int(sys.argv[1])
    print(f"Finding {job_id}")
    print(docker.find_associated_to_job(job_id))
