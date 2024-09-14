from typing import ClassVar
import subprocess
import logging
import json

logger = logging.getLogger(__name__)

class Slurm:
    API_PREFIX: ClassVar[str] = "/slurm/v0.0.37"
    RESTD_BIN: ClassVar[str] = "/cm/shared/apps/slurm/current/sbin/slurmrestd"

    _RESTD_BIN_HINTS: ClassVar[list[str]] = ["slurmrestd", "/cm/shared/apps/slurm/current/sbin/slurmrestd"]

    @classmethod
    def ensure_restd(cls):
        for hint in cls._RESTD_BIN_HINTS:
            path = cls.cmd_is_available(hint)
            if path:
                cls.RESTD_BIN = path.strip()
                return cls.RESTD_BIN
        raise RuntimeError("Slurm: could not find 'slurmrestd' on this system")

    @staticmethod
    def get_user():
        return (
            subprocess.run("whoami", stdout=subprocess.PIPE).stdout.decode("utf-8").strip()
        )

    @staticmethod
    def cmd_is_available(cmd: str) -> str | None:
        response = subprocess.run(f"command -v {cmd}", shell=True, stdout=subprocess.PIPE, stderr=None)
        if response.returncode == 0:
            return response.stdout.decode("UTF-8")
        return None

    @classmethod
    def get_slurmrestd(cls, prefix: str):
        if not prefix.startswith("/"):
            prefix = f"/{prefix}"

        msg = f'echo -e "GET {cls.API_PREFIX}{prefix} HTTP/1.1\r\n" | {cls.RESTD_BIN} -a rest_auth/local'
        logger.debug(f"Query: {msg}")
        response = subprocess.run(msg, shell=True, stdout=subprocess.PIPE).stdout.decode(
            "utf-8"
        )
        header, content = response.split("{", 1)
        json_data = json.loads("{" + content)
        logger.debug(f"Response: {json_data}")
        return json_data

    @classmethod
    def get_node_names(cls) -> list[str]:
        nodes_data = cls.get_slurmrestd("/nodes")
        return [node["name"] for node in nodes_data["nodes"]]
