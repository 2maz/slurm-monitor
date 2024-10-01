from pathlib import Path
import subprocess
import shutil

class Command:
    @classmethod
    def find(cls, *, command, hints: list[str] | None = None, do_throw = True ) -> str | None:
        search_paths = [ Path(command) ]
        if hints:
            for x in hints:
                search_paths.append(Path(x) / command)

        for search_path in search_paths:
            path = shutil.which(cmd=search_path)
            if path:
                return path
        if do_throw:
            raise RuntimeError(f"Command: could not find '{command}' on this system")

        return None

    @classmethod
    def run(cls, command: str) -> str:
        response = subprocess.run(command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)

        if response.returncode == 0:
            return response.stdout.decode("utf-8").strip()

        raise RuntimeError(
                f"Command.run: '{command}' failed with returncode: "
                f"{response.returncode} - {response.stderr}"
                )

    @classmethod
    def get_user(cls) -> str:
        return cls.run("whoami").strip()
