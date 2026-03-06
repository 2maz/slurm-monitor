#!/usr/bin/bash

PYTHON_VERSION=${PYTHON_VERSION:-3.12}
SLURM_MONITOR_TAG=${SLURM_MONITOR_TAG:-main}

echo "USING:"
echo "    PYTHON_VERSION=$PYTHON_VERSION"
echo "    SLURM_MONITOR_TAG=$SLURM_MONITOR_TAG"

if ! command -v mise; then
    curl https://mise.run | sh
    eval "$(~/.local/bin/mise activate bash)"
else
    echo "mise-en-place: already installed"
fi

if [ ! -d venv-$PYTHON_VERSION ]; then
    mise use python@$PYTHON_VERSION
    mise exec python@$PYTHON_VERSION -- python -m venv venv-$PYTHON_VERSION
else
    echo "venv-$PYTHON_VERSION: already exists"
fi

if [ ! -d slurm-monitor ]; then
    git clone -b $SLURM_MONITOR_TAG https://github.com/2maz/slurm-monitor.git
else
    echo "slurm-monitor: already checked out"
fi

. venv-$PYTHON_VERSION/bin/activate
python -m pip install ./slurm-monitor

grep -q "mise activate bash" ~/.bashrc
if [ $? -ne 0 ]; then
    echo 'eval "$(~/.local/bin/mise activate bash)"' >> ~/.bashrc
else
    echo ".bashrc: already contains mise activate"
fi

grep -q "venv-$PYTHON_VERSION" ~/.bashrc
if [ $? -ne 0 ]; then
    echo "eval \"\$(. /home/docker/venv-$PYTHON_VERSION/bin/activate)\"" >> ~/.bashrc
else
    echo ".bashrc: already contains activation of venv-$PYTHON_VERSION"
fi
