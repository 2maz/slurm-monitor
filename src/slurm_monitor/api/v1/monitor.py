#from slurm_monitor.backend.worker import celery_app
from fastapi import APIRouter

logger: Logger = getLogger(__name__)

api_router = APIRouter(
    prefix="/monitor",
    tags=["monitor"],
)


@api_router.get("/monitor/jobs", response_model=None)
async def jobs():
    """
    Check status of a task
    Example:
        http://127.0.0.1:8000/admin/tasks/<task_id>/cancel

    see https://docs.celeryq.dev/en/stable/userguide/workers.html#revoke
    """
    jobs = { 'jobs': ["main"] }
    return jobs
