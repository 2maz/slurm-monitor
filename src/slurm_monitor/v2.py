#!/usr/bin/env python3

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, exception_handlers, HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend
from fastapi_utils.tasks import repeat_every

from starlette.exceptions import HTTPException as StarletteHTTPException
from prometheus_fastapi_instrumentator import Instrumentator
import traceback
import gc


from slurm_monitor.app_settings import AppSettings
from slurm_monitor.db_operations import DBManager
from slurm_monitor.api.v2.router import app as api_v2_app
from slurm_monitor.utils.api import find_endpoint_by_name

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
    logging.basicConfig(
        format='[{asctime}][{levelname:^8s}] {name}: {message}',
        style='{',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO,
    )

    logger.setLevel(logging.DEBUG)  # output of exception handlers above
    logger.info("Setting up cache ...")
    FastAPICache.init(
            backend=InMemoryBackend(),
            prefix="fastapi-cache"
    )

    logger.info("Setting up database ...")
    app_settings = AppSettings.initialize(db_schema_version="v2")
    if app_settings.prefetch.enabled:
        logger.info("Setting up prefetching ...")

        # Keeping it here to parametrize via app_settings
        repeat_decorator = repeat_every(seconds=app_settings.prefetch.interval, logger=logger)
        task = asyncio.create_task(repeat_decorator(prefetch_data)(), name="prefetch_data")

    yield

    if app_settings.prefetch.enabled:
        task.cancel("Application is stopping")
        try:
            await task
        except asyncio.CancelledError:
            logger.info("Prefetching has been stopped")
    logger.info("Shutting down ...")

tags_metadata = [
    {
        "name": "cluster",
        "description": "Operations that give a high-level **cluster**-specific result",

    },
    {
        "name": "node",
        "description": "Operations that give **node**-specific results",
        "externalDocs": {
            "description": "test external",
            "url": "https://fastapi.tiangolo.com",
        },

    },
    {
        "name": "job",
        "description": "Operations that give **job**-specific results",

    },
]

app = FastAPI(
    title="slurm-monitor", description="slurm monitor", version="0.2", lifespan=lifespan,
    openapi_tags=tags_metadata
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

    raise HTTPException(status_code=500,
            detail=f"Internal Error: {exc}")

async def prefetch_data():
    logger.info("Prefetch starting")
    dbi = DBManager.get_database()
    cluster_endpoint = find_endpoint_by_name(app=app, name="cluster")
    clusters = await cluster_endpoint(token_payload=None, dbi=dbi)

    nodes_sysinfo_endpoint = find_endpoint_by_name(app=app, name="nodes_sysinfo")
    partitions_endpoint = find_endpoint_by_name(app=app, name="partitions")
    jobs_endpoint = find_endpoint_by_name(app=app, name="jobs")

    for cluster_data in clusters:
        cluster = cluster_data['cluster']

        # DO NOT use a dynamic argument such as time_in_s, since that will
        # prevent the caching to work
        try:
            await nodes_sysinfo_endpoint(token_payload=None, cluster=cluster, time_in_s=None, dbi=dbi)
            await partitions_endpoint(token_payload=None, cluster=cluster, time_in_s=None, dbi=dbi)
            await jobs_endpoint(token_payload=None, cluster=cluster, dbi=dbi)
        except Exception:
            logger.info("Prefetching failed - {e}")

    logger.info("Prefetching done")

    # Run the garbage collection explicitly to avoid
    # early memory exhaustion
    logger.debug("Running gc")
    gc.collect()
    logger.debug(f"Done running gc, post stats: {gc.get_stats()}")

# Serve API. We want the API to take full charge of its prefix, not involve the SPA mount
# at all, hence we use a submount rather than subrouter.
app.mount(path=api_v2_app.root_path, app=api_v2_app, name="api/v2")
