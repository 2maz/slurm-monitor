#!/bin/bash

PORT=${1:-7777}

export SLURM_MONITOR_JOBS_COLLECTOR=false
export SLURM_MONITOR_USE_SLURM=false

SLURM_MONITOR_DATABASE_URI=$(slurm-monitor test --port $PORT)
export SLURM_MONITOR_DATABASE_URI

python3 -m uvicorn --reload slurm_monitor.main:app --port 12001 --host 0.0.0.0
