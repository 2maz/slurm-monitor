FROM debian:bookworm-slim

ARG PYTHON_VERSION="3.14"
ARG SLURM_MONITOR_TAG="main"

ENV PYTHON_VERSION=$PYTHON_VERSION
ENV SLURM_MONITOR_TAG=$SLURM_MONITOR_TAG

WORKDIR /home/docker

COPY ./prepare-docker.sh .
COPY ./prepare-slurm-monitor.sh .

RUN ./prepare-docker.sh

USER docker
RUN ./prepare-slurm-monitor.sh

ENV VIRTUAL_ENV=/home/docker/venv-${PYTHON_VERSION}
ENV VIRTUAL_ENV_PROMPT=venv-${PYTHON_VERSION}
ENV PATH=$VIRTUAL_ENV/bin:$PATH
