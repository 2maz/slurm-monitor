from pydantic import BaseModel, NonNegativeInt


class ProcessStats(BaseModel):
    # actual os process id
    pid: NonNegativeInt

    cpu_percent: float = 0.0
    memory_percent: float = 0.0
