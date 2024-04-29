from fastapi import FastAPI

from . import monitor

app = FastAPI(root_path="/api/v1")

@app.get("/")
async def hello():
    return {"message": "Slurm Monitor API v1"}

app.include_router(monitor.api_router)
