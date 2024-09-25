from __future__ import annotations

from psutil import Process, NoSuchProcess
from typing import ClassVar
import re
import subprocess
from pydantic import (
        BaseModel,
        NonNegativeInt
)
from pydantic_settings import BaseSettings
import logging
from functools import reduce
from slurm_monitor.utils.slurm import Slurm

logger = logging.getLogger(__name__)

class ProcessStats(BaseModel):
    # actual os process id
    pid: NonNegativeInt

    cpu_percent: float = 0.0
    memory_percent: float = 0.0

class JobStats(BaseModel):
    cpu_percent: float = 0.0
    memory_percent: float = 0.0

    def __add__(self, other) -> JobStats:
        self.cpu_percent += other.cpu_percent
        self.memory_percent += other.memory_percent
        return self

class JobList(BaseSettings):
    # map from slurm jobs to processes
    jobs: dict[int, list[ProcessStats]] = {}

    def get_job_stats(self, job_id) -> JobStats:
        """
            Compute the aggregated stats per for a single slurm job
        """
        if job_id not in self.jobs:
            raise KeyError(f"Job {job_id} does not exist")

        def add_fn(a,b):
            return a+b

        return reduce(add_fn, self.jobs[job_id], JobStats())


class JobMonitor:
    processes: ClassVar[Process] = {}

    @classmethod
    def get_process(cls, pid) -> Process:
        if pid not in cls.processes:
            p = Process(pid)
            cls.processes[pid] = p
            return p

        return cls.processes[pid]

    @classmethod
    def get_active_jobs(cls):
        active_jobs: JobList = JobList()

        Slurm.ensure_commands()

        cmd = f"{Slurm.SCONTROL} listpids"
        response = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if response.returncode != 0:
            error_msg = response.stderr.decode('UTF-8').strip()
            if re.match("No job steps", error_msg):
                return active_jobs
            elif re.match("Unable to connect to slurmstepd", error_msg):
                pass
            else:
                raise RuntimeError(f"Calling {cmd} failed - {error_msg}")

        # PID JOBID STEPID LOCALID GLOBALID
        lines = response.stdout.decode("UTF-8").strip().splitlines()

        if len(lines) > 0:
            for line in lines[1:]:
                try:
                    pid, job_id, _, _, _ = line.split()

                    pid = int(pid)
                    job_id = int(job_id)

                    process_description = ProcessStats(pid=pid)
                    try:
                        p = cls.get_process(process_description.pid)
                        with p.oneshot():
                            process_description.cpu_percent = p.cpu_percent()
                            process_description.memory_percent = p.memory_percent()

                        if job_id not in active_jobs.jobs:
                            active_jobs.jobs[job_id] = [ process_description ]
                        else:
                            active_jobs.jobs[job_id].append(process_description)
                    except NoSuchProcess:
                        logger.debug(f"JobMonitor.get_active_jobs: no process with {pid=}")
                        pass
                except Exception as e:
                    logger.warning(f"Line {line} does not match the expected format - {e}")
                    break

        return active_jobs
