from slurm_monitor.utils.api import createFastAPI

# register routes under tag
from . import (
    cluster,  # noqa
    jobs,  # noqa
    monitor,  # noqa
    nodes,  # noqa
    routes,
    user,  # noqa
)

app = createFastAPI(
    title="slurm-monitor REST API",
    version="2",
    root_path="/api/v2",
)


@app.get("/")
async def hello():
    return {"message": "Slurm Monitor API v2"}


app.include_router(routes.api_router)
