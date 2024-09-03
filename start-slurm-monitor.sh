#!/usr/bin/bash -l

SCRIPT_DIR=$(dirname $(realpath -L $0))

SSL_CERT_DIR=${SSL_CERT_DIR:-$SCRIPT_DIR/tests/data/ssl}

SSL_KEY_FILE=$SSL_CERT_DIR/key.pem
SSL_CERT_FILE=$SSL_CERT_DIR/cert.pem

if [ ! -e $SSL_KEY_FILE ]; then
    echo "Error: missing SSL_KEY_FILE '$SSL_KEY_FILE'"
    exit 10
fi
if [ ! -e $SSL_CERT_FILE ]; then
    echo "Error: missing SSL_CERT_FILE '$SSL_CERT_FILE'"
    exit 11
fi

SLURM_MONITOR_HOST=${SLURM_MONITOR_HOST:-0.0.0.0}
DB_BASE_DIR=${DB_BASE_DIR:-$HOME/.slurm-monitor}
DB_HOME="sqlite:///$DB_BASE_DIR"

mkdir -p $DB_BASE_DIR
if [ "$1" == 'dev' ]; then
    echo "Running in development mode:"
    SLURM_MONITOR_DATABASE_URI="$DB_HOME/slurm-monitor-db.dev.sqlite"
    PORT=12001
else
    echo "Running in production mode:"
    SLURM_MONITOR_DATABASE_URI="$DB_HOME/slurm-monitor-db.sqlite"
    SLURM_MONITOR_HOST=$SLURM_MONITOR_HOST
    PORT=12000
fi

echo "    SLURM_MONITOR_DATABASE_URI=$SLURM_MONITOR_DATABASE_URI"
echo "    HOST:PORT=$SLURM_MONITOR_HOST:$PORT"

export SLURM_MONITOR_DATABASE_URI
python3 -m uvicorn --reload slurm_monitor.main:app --port $PORT --host $SLURM_MONITOR_HOST --ssl-keyfile $SSL_KEY_FILE --ssl-certfile $SSL_CERT_FILE > slurm-monitor.main.log 2>&1 &
