from fastapi import FastAPI

from . import monitor

app = FastAPI(root_path="/api/v2")


@app.get("/")
async def hello():
    return {"message": "Slurm Monitor API v2"}


app.include_router(monitor.api_router)
