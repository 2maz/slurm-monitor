from typing import ClassVar
import logging
import json
import subprocess
import re

from slurm_monitor.utils.command import Command

logger = logging.getLogger(__name__)

SCALE_BY_UNIT = {
    "K": 1024,
    "M": 1024**2,
    "G": 1024**3,
    "T": 1024**4,
    "P": 1024**5,
}

TRES_KEYS = ["cpu", "mem", "gpu", "node", "billing"]
TRES_PATTERN = r"([^/]+)(:[^=]+)?=([0-9.]+)([" + "".join(SCALE_BY_UNIT.keys()) + "])?"
TRES_REGEXP = re.compile(TRES_PATTERN)

COMPACT_NODE_EXPRESSION_PATTERN: str = r"(.*)\[(.*)\](\..+){0,}$"
COMPACT_NODE_EXPRESSION_REXEXP = re.compile(COMPACT_NODE_EXPRESSION_PATTERN)


class Slurm:
    API_PREFIX: ClassVar[str] = "/slurm/v0.0.37"

    SLURMRESTD = "slurmrestd"
    SCONTROL = "scontrol"

    _BIN_HINTS: ClassVar[list[str]] = [
        "/cm/shared/apps/slurm/current/sbin/",
        "/cm/shared/apps/slurm/current/bin/",
    ]
    _ensured_commands: ClassVar[list[str]] = []

    @classmethod
    def ensure_commands(cls):
        for cmd in ["slurmrestd", "scontrol"]:
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
        response = subprocess.run(
            cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        if response.returncode != 0:
            error_msg = response.stderr.decode("UTF-8").strip()
            if re.match("No job steps", error_msg) or re.match(
                ".*no steps.*", error_msg
            ):
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
                        logger.warning(
                            f"{current_job_id=} does not match requested {job_id=} - skipping"
                        )
                        continue

                    pids.append(pid)
                except Exception as e:
                    logger.warning(
                        f"Line '{line}' does not match the expected format - {e}"
                    )
                    continue
        return pids

    @classmethod
    def parse_sacct_tres(cls, txt: str) -> dict[str, int | float]:
        """
        Parse TRES and return a dictionary mapping cpu, mem, gpu to the actual count
        """
        values = {x: 0 for x in TRES_KEYS}

        for field in txt.split(","):
            m = TRES_REGEXP.search(field)
            if m and m.groups():
                key = m.groups()[0]
                if key not in TRES_KEYS:
                    logger.debug(f"{key} is not a default used TRES field")

                # check details, e.g., gres/gpu:modelname=1 -> gpu:modelname
                specifier = m.groups()[1]
                if specifier is not None:
                    key += specifier

                value = m.groups()[2]
                if "." in value:
                    values[key] = float(value)
                else:
                    values[key] = int(value)

                # apply unit based scaling
                unit = m.groups()[3]
                if not unit:
                    continue

                values[key] *= SCALE_BY_UNIT[unit]
            else:
                logger.info(
                    f"Slurm.parse_sacct_tres: invalid pattern encountered {txt}"
                )

        return values

    @classmethod
    def expand_node_names(cls, names: str) -> list[str]:
        """
        Handle and expand compact node patterns: n[001-002,004],g[001-g003].domain,n001
        into single node names
        """
        nodes = []
        if type(names) is not str:
            raise TypeError(
                f"Importer.expand_node_names: expects names to be str, but was '{type(names)}'"
            )

        current_expr = None
        for chunk in names.split(","):
            if not current_expr:
                current_expr = chunk
            else:
                current_expr += "," + chunk

            if "[" in current_expr:
                if "]" not in current_expr:
                    continue

            m = COMPACT_NODE_EXPRESSION_REXEXP.match(current_expr)
            if m is None:
                nodes.append(current_expr)
                current_expr = None
                continue

            prefix, suffixes = m.groups()[:2]
            domain = m.groups()[2]
            # [005-006,001,005]
            for suffix in suffixes.split(","):
                # 0,0
                if "-" not in suffix:
                    nodename = f"{prefix}{suffix}"
                    if domain:
                        nodename = f"{nodename}{domain}"
                    nodes.append(nodename)
                    current_expr = None
                    continue

                # 001-010
                start, end = suffix.split("-")
                pattern_length = len(start)

                for i in range(int(start), int(end) + 1):
                    node_number = str(i).zfill(pattern_length)
                    nodename = f"{prefix}{node_number}"
                    if domain:
                        nodename = f"{nodename}{domain}"
                    nodes.append(nodename)
                    current_expr = None

        return nodes
