from typing import ClassVar
import logging
import json
import subprocess
import re

from slurm_monitor.utils.command import Command

logger = logging.getLogger(__name__)

class Slurm:
    API_PREFIX: ClassVar[str] = "/slurm/v0.0.37"

    SLURMRESTD = "slurmrestd"
    SCONTROL = "scontrol"

    _BIN_HINTS: ClassVar[list[str]] = ["/cm/shared/apps/slurm/current/sbin/", "/cm/shared/apps/slurm/current/bin/"]
    _ensured_commands: ClassVar[list[str]] = []

    @classmethod
    def ensure_commands(cls):
        for cmd in ["slurmrestd","scontrol"]:
            cls.ensure(cmd)

    @classmethod
    def ensure(cls, cmd: str):
        if cmd not in cls._ensured_commands:
            setattr(cls, cmd.upper(), Command.find(command=cmd, hints=cls._BIN_HINTS))
            cls._ensured_commands.extend(cmd)
        return getattr(cls, cmd.upper())

    @classmethod
    def get_slurmrestd(cls, prefix: str):
        if not prefix.startswith("/"):
            prefix = f"/{prefix}"

        cmd = f'echo -e "GET {cls.API_PREFIX}{prefix} HTTP/1.1\r\n" | {cls.SLURMRESTD} -a rest_auth/local'
        response = Command.run(command=cmd)

        header, content = response.split("{", 1)
        json_data = json.loads("{" + content)
        logger.debug(f"Response: {json_data}")
        return json_data

    @classmethod
    def get_node_names(cls) -> list[str]:
        nodes_data = cls.get_slurmrestd("/nodes")
        return [node["name"] for node in nodes_data["nodes"]]

    @classmethod
    def get_associated_pids(cls, job_id: int) -> list[int]:
        pids = []
        try:
            scontrol = Slurm.ensure("scontrol")
        except RuntimeError:
            logger.warning("Slurm.get_associated_pids: scontrol is not available")
            return pids

        cmd = f"{scontrol} listpids {job_id}"
        response = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if response.returncode != 0:
            error_msg = response.stderr.decode('UTF-8').strip()
            if re.match("No job steps", error_msg) or re.match(".*no steps.*", error_msg):
                return pids
            elif re.match("Unable to connect to slurmstepd", error_msg):
                pass
            else:
                raise RuntimeError(f"Calling {cmd} failed - {error_msg}")

        # PID JOBID STEPID LOCALID GLOBALID
        lines = response.stdout.decode("UTF-8").strip().splitlines()
        if len(lines) > 0:
            for line in lines[1:]:
                try:
                    pid, current_job_id, _, _, _ = line.split()

                    pid = int(pid)
                    current_job_id = int(current_job_id)

                    if pid < 0:
                        logger.warning(f"{pid=} invalid - skipping")
                        continue

                    if job_id != current_job_id:
                        logger.warning(f"{current_job_id=} does not match requested {job_id=} - skipping")
                        continue

                    pids.append(pid)
                except Exception as e:
                    logger.warning(f"Line '{line}' does not match the expected format - {e}")
                    continue
        return pids

    @classmethod
    def parse_sacct_tres(cls, txt: str) -> dict[str, int]:
        """
        Parse TRES and return a dictionary mapping cpu,mem,gpu to the actual count
        """
        values = {}
        for key in ["cpu", "mem", "gpu", "node", "billing"]:
            values[key] = 0
            m = re.search(key + "=([^,]+)", txt)
            if m and m.groups():
                values[key] = int(m.groups()[0])

        return values
