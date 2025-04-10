#!/bin/bash

TESTS_DIR=$(realpath -L "$(dirname $0)/..")

if [ "$1" == "" ]; then
    IMPORT_FILES=""
    for suffix in "sysinfo" "ps"; do
        for node_id in "fox" "ml1"; do
            IMPORT_FILES="$IMPORT_FILES $TESTS_DIR/data/db/v2/$node_id-$suffix.json"
        done

        # ex3
        for node_id in "g001" "g002"; do
            json_files=$(ls $TESTS_DIR/data/db/v2/ex3/ex3-$node_id-$suffix*.json)
            IMPORT_FILES="$IMPORT_FILES $json_files"
        done
    done
else
    IMPORT_FILES=$@
fi

echo "Loading $IMPORT_FILES"
CONTAINER_NAME=slurm-monitor-test-timescaledb

if [ ! -e $IMPORT_FILE ] ; then
    echo "File $IMPORT_FILE does not exist"
    exit 10
fi


docker stop $CONTAINER_NAME
docker run -d --rm --name $CONTAINER_NAME -p 7777:5432 -e POSTGRES_DB=test -e POSTGRES_PASSWORD=test -e POSTGRES_USER=test timescale/timescaledb:latest-pg17
SLURM_MONITOR_DATABASE_URI=timescaledb://test:test@localhost:7777/test

#HOSTNAME=srl-login3.cm.cluster
export SLURM_MONITOR_DATA_DIR=/tmp/slurm-monitor-data

echo "Waiting for db to start - 10 seconds"
sleep 10

for file in $IMPORT_FILES; do
    echo "slurm-monitor import --db-uri $SLURM_MONITOR_DATABASE_URI --file $file"
    slurm-monitor import --db-uri $SLURM_MONITOR_DATABASE_URI --file $file
done

# Will continue the existing timeseries data to the current time
echo "Consider to complete db with:"
echo "    slurm-monitor import --db-uri $SLURM_MONITOR_DATABASE_URI --fake-timeseries"

echo ""
echo "Running on $SLURM_MONITOR_DATABASE_URI"
echo "Call 'docker stop $CONTAINER_NAME' to stop container if needed"
