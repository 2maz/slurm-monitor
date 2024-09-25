from typing import ClassVar
import logging
import json

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
