import shutil
import subprocess
from pathlib import Path


class Command:
    @classmethod
    def find(cls, *, command: str, hints: list[str] | None = None, do_throw: bool = True) -> str | None:
        search_paths: list[Path] = []
        if hints:
            for x in hints:
                search_paths.append(Path(x) / command)

        # default
        search_paths.append(Path(command))

        for search_path in search_paths:
            path = shutil.which(cmd=search_path)
            if path:
                return path
        if do_throw:
            raise RuntimeError(f"Command: could not find '{command}' on this system")

        return None

    @classmethod
    def run_and_get_exit_code(cls, command: str) -> bool:
        try:
            _ = subprocess.run(command,
                    shell=True,
                    check=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL)
            return True
        except subprocess.CalledProcessError:
            return False

    @classmethod
    def run(cls, command: str, decode: str | None = 'utf-8', timeout: int | None = None) -> str | bytes:
        try:
            response = subprocess.run(command,
                    shell=True,
                    check=True,
                    timeout=timeout,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            raise RuntimeError(f"Command.run: '{command}' failed") from e

        if decode is None:
            return response.stdout
        else:
            return response.stdout.decode(decode).strip()

    @classmethod
    def get_user(cls) -> str:
        return str(cls.run("whoami")).strip()
