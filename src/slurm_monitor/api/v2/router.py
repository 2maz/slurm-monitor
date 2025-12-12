from fastapi import FastAPI

from . import routes

# register routes under tag
from . import monitor # noqa
from . import cluster # noqa
from . import nodes   # noqa
from . import jobs    # noqa

app = FastAPI(root_path="/api/v2")

@app.get("/")
async def hello():
    return {"message": "Slurm Monitor API v2"}


app.include_router(routes.api_router)
