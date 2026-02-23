#!/usr/bin/env python3

from __future__ import annotations

from contextlib import asynccontextmanager
import os


from fastapi import FastAPI, Request, exception_handlers, HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend

from starlette.exceptions import HTTPException as StarletteHTTPException
from prometheus_fastapi_instrumentator import Instrumentator
import traceback


from slurm_monitor.utils.slurm import Slurm
from slurm_monitor.app_settings import AppSettings

from slurm_monitor.api.v1.router import app as api_v1_app
from slurm_monitor.db.v1.data_collector import start_jobs_collection
from slurm_monitor.db.v1.db import SlurmMonitorDB

import logging
from logging import getLogger

logger = getLogger(__name__)
logger.setLevel(logging.INFO)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Define lifespan of application, as described in:
    https://fastapi.tiangolo.com/advanced/events/#lifespan
    """
    logging.basicConfig(level=logging.INFO)
    logger.setLevel(logging.DEBUG)  # output of exception handlers above
    logger.info("Setting up cache ...")
    FastAPICache.init(InMemoryBackend(), prefix="fastapi-cache")

    logger.info("Setting up database ...")
    app_settings = AppSettings.initialize(db_schema_version="v1", force=True)

    database = SlurmMonitorDB(app_settings.database)

    # First make sure that this runs on a slurm system
    do_use_slurm = True
    if "SLURM_MONITOR_USE_SLURM" in os.environ:
        if os.environ["SLURM_MONITOR_USE_SLURM"].lower() == "false":
            do_use_slurm = False

    if do_use_slurm:
        Slurm.ensure_commands()

    do_collect_jobs = True
    if "SLURM_MONITOR_JOBS_COLLECTOR" in os.environ:
        if os.environ["SLURM_MONITOR_JOBS_COLLECTOR"].lower() == "false":
            do_collect_jobs = False

    jobs_pool = None
    if do_collect_jobs:
        logger.info("Startup of jobs collector")
        jobs_pool = start_jobs_collection(database)
    else:
        logger.warning(
            "Jobs collector has been disabled (via env SLURM_MONITOR_JOBS_COLLECTOR)"
        )

    yield

    logger.info("Shutting down ...")
    if jobs_pool is not None:
        jobs_pool.stop()


app = FastAPI(
    title="slurm-monitor", description="slurm monitor", version="0.1", lifespan=lifespan
)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,  # Can be set to true if we specify origin
    allow_methods=["*"],
    allow_headers=["*"],
)  # This is called before any FastAPI Request (Cross-Origin Resource Sharing), i.e.
# when the JavaScript code in front-end communicates with the backend

# Instrumenting prometheus metrics on /metrics
Instrumentator().instrument(app).expose(app)


# see https://fastapi.tiangolo.com/tutorial/handling-errors/#fastapis-httpexception-vs-starlettes-httpexception
@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    if exc.status_code not in [404]:
        logger.debug("", exc_info=exc)
    return await exception_handlers.http_exception_handler(request, exc)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    logger.debug("", exc_info=exc)
    return await exception_handlers.request_validation_exception_handler(request, exc)


@app.exception_handler(Exception)
async def runtime_exception_handler(request: Request, exc: Exception):
    logger.warning(exc)
    traceback.print_tb(exc.__traceback__)

    raise HTTPException(status_code=500, detail=f"Internal Error: {exc}")


# Serve API. We want the API to take full charge of its prefix, not involve the SPA mount
# at all, hence we use a submount rather than subrouter.
app.mount(api_v1_app.root_path, api_v1_app)
