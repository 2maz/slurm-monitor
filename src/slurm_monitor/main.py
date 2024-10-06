#!/usr/bin/env python3

from __future__ import annotations

from contextlib import asynccontextmanager


from fastapi import FastAPI, Request, exception_handlers
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend

from starlette.exceptions import HTTPException as StarletteHTTPException
from prometheus_fastapi_instrumentator import Instrumentator

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
    app_settings = AppSettings.initialize()
    database = SlurmMonitorDB(app_settings.database)

    # First make sure that this runs on a slurm system
    Slurm.ensure_commands()
    logger.info("Startup of jobs collector")
    jobs_pool = start_jobs_collection(database)

    yield

    logger.info("Shutting down ...")
    jobs_pool.stop()



AppSettings.initialize()

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


# Serve API. We want the API to take full charge of its prefix, not involve the SPA mount
# at all, hence we use a submount rather than subrouter.
app.mount(api_v1_app.root_path, api_v1_app)
