#!/usr/bin/env python3

from __future__ import annotations

from contextlib import asynccontextmanager

from pathlib import Path
import yaml
import subprocess
import os
import sys
import time

from fastapi import FastAPI
from fastapi import FastAPI, HTTPException, Request, exception_handlers
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from starlette.exceptions import HTTPException as StarletteHTTPException


from .api.v1 import app as api_v1_app

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
    logger.info("Connecting ...")
    yield
    # Do nothing at shutdown
    logger.info("Shutting down ...")

app = FastAPI(
        title="slurm-monitor",
        description="slurm monitor",
        version="0.1",
        lifespan=lifespan
)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,  # Can be set to true if we specify origin
    allow_methods=["*"],
    allow_headers=["*"],
)  # This is called before any FastAPI Request (Cross-Origin Resource Sharing), i.e.
# when the JavaScript code in front-end communicates with the backend


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
