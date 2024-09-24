from typing import ClassVar
import subprocess
import logging
import json

from slurm_monitor.utils.command import Command

logger = logging.getLogger(__name__)

class Slurm:
    API_PREFIX: ClassVar[str] = "/slurm/v0.0.37"

    SLURMRESTD = "slurmrestd"
    SCONTROL = "scontrol"

    _BIN_HINTS: ClassVar[list[str]] = ["/cm/shared/apps/slurm/current/sbin/"]
    _ensured_commands: ClassVar[bool] = False

    @classmethod
    def ensure_commands(cls):
        if not cls._ensured_commands:
            cls.SLURMRESTD = Command.find(command="slurmrestd", hints=cls._BIN_HINTS)
            cls.SCONTROL = Command.find(command="scontrol", hints=cls._BIN_HINTS)

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
