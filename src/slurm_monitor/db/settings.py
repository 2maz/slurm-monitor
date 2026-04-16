from pydantic import BaseModel
import os

class DatabaseSettings(BaseModel):
    user: str | None = None
    password: str | None = None
    uri: str = f"sqlite:///{os.environ['HOME']}/.slurm-monitor/slurm-monitor-db.sqlite"

    create_missing: bool = True

