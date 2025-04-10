#!/bin/bash

PORT=${1:-7777}

export SLURM_MONITOR_JOBS_COLLECTOR=false
export SLURM_MONITOR_DATABASE_URI=timescaledb://test:test@localhost:$PORT/test
export SLURM_MONITOR_USE_SLURM=false

python3 -m uvicorn --reload slurm_monitor.main:app --port 12001 --host 0.0.0.0

