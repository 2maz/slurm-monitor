#from slurm_monitor.backend.worker import celery_app
from fastapi import APIRouter
from logging import getLogger, Logger
import subprocess
import json

logger: Logger = getLogger(__name__)

SLURM_API_PREFIX="/slurm/v0.0.36"

api_router = APIRouter(
    prefix="/monitor",
    tags=["monitor"],
)

def _get_slurmrestd(prefix: str):
    if not prefix.startswith("/"):
        prefix = f"/{prefix}"

    msg = f"echo -e \"GET {SLURM_API_PREFIX}{prefix} HTTP/1.1\r\n\" | slurmrestd -a rest_auth/local"
    logger.info(f"Query: {msg}")
    response = subprocess.run(msg, shell=True, stdout=subprocess.PIPE).stdout.decode('utf-8')
    header, content = response.split("\r\n\r\n",1)
    json_data = json.loads(content)
    logger.info(f"Response: {json_data}")
    return json_data


@api_router.get("/jobs", response_model=None)
async def jobs():
    """
    Check status of jobs
    """
    return _get_slurmrestd("/jobs")

@api_router.get("/nodes", response_model=None)
async def nodes():
    """
    Check status of nodes
    """
    return _get_slurmrestd("/nodes")

@api_router.get("/partitions", response_model=None)
async def jobs():
    """
    Check status of partitions
    """
    return _get_slurmrestd("/partitions")
