#!/usr/bin/bash -i

SCRIPT_DIR=$(dirname $(realpath -L $0))

SSL_CERT_DIR=$SCRIPT_DIR/tests/data/ssl
SSL_KEY_FILE=$SSL_CERT_DIR/key.pem
SSL_CERT_FILE=$SSL_CERT_DIR/cert.pem

DB_HOME="sqlite:///$HOME/.slurm-monitor"
mkdir -p $HOME

if [ "$1" == 'dev' ]; then
    echo "Running in development mode"
    SLURM_MONITOR_DATABASE_URI="$DB_HOME/slurm-monitor-db.dev.sqlite"
    PORT=12001
else
    echo "Running in production mode"
    SLURM_MONITOR_DATABASE_URI="$DB_HOME/slurm-monitor-db.sqlite"
    PORT=12000
fi

if [ "$VENV_NAME" != "slurm-monitor" ]; then
    venv-activate slurm-monitor-dev
fi

export SLURM_MONITOR_DATABASE_URI
python3 -m uvicorn --reload slurm_monitor.main:app --port $PORT --host 0.0.0.0 --ssl-keyfile $SSL_KEY_FILE --ssl-certfile $SSL_CERT_FILE
