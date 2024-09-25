from pathlib import Path
import subprocess
import shutil

class Command:
    @classmethod
    def find(cls, *, command, hints: list[str] | None) -> str:
        search_paths = [ Path(command) ]
        for x in hints:
            search_paths.append(Path(x) / command)

        for search_path in search_paths:
            path = shutil.which(cmd=search_path)
            if path:
                return str(path.resolve()).strip()

        raise RuntimeError(f"Command: could not find '{command}' on this system")

    @classmethod
    def run(cls, command: str) -> str:
        response = subprocess.run(command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)

        if response.returncode == 0:
            return response.stdout.decode("utf-8")

        raise RuntimeError(
                f"Command.run: '{command}' failed with returncode: "
                f"{response.returncode} - {response.stderr}"
                )

    @classmethod
    def get_user(cls) -> str:
        return cls.run("whoami")
