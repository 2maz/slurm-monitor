# Slurm Monitor

This package provides mainly three components for monitoring GPU and CPU resource in (Slurm) cluster:

1. a probe that publishes monitoring data to a kafka broker
1. a listener that collects monitoring data a writes it to a database (sqlite or timeseriesdb)
1. a REST Api to make the data accessible, so that it can, for instance, be used with a [web frontend](https://github.com/2maz/slurm-monitor-frontend)

If being an admin and requiring a detailled generic monitoring to be available with [Performance Co-Pilot](pcp.io), this library is being used as
quick approach to include recent GPU devices into the monitoring.
GPU data is being collected through vendor's smi tools, although pynvml and pyrsmi can be used as well.

This library currently include monitoring data by quering the following:
 - [nvidia-smi](https://docs.nvidia.com/deploy/nvidia-smi/index.html)
 - [rocm-smi](https://github.com/ROCm/rocm_smi_lib/tree/master/python_smi_tools)
 - [hl-smi](https://docs.habana.ai/en/latest/Management_and_Monitoring/Embedded_System_Tools_Guide/System_Management_Interface_Tool.html)
 - [xpu-smi](https://intel.github.io/xpumanager/smi_user_guide.html)


## Usage

The current setup uses kafka and (optionally) timescaledb.
To run kafka (and timescaledb) you can use the following docker compose file as a starting point, i.e., to be customized for your needs and your environment:

Create an .env file with the hostname where the kafka instance will run, this will be used for advertising the endpoint so do not use localhost here, e.g.,
 use the output of +hostname+ on linux.

.env file:

```
    HOSTNAME=<kafka-brokers-hostname>
```

The docker-compose.yaml looks as follows:

```
version: "3.9"
services:
  kafka-broker:
    image: apache/kafka:latest
    hostname: kafka-broker
    container_name: kafka-broker
    ports:
      - 10092:10092
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-broker:29092,PLAINTEXT_HOST://${HOSTNAME}:10092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_NODE_ID: 1
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka-broker:29093
      KAFKA_LISTENERS: PLAINTEXT://kafka-broker:29092,CONTROLLER://kafka-broker:29093,PLAINTEXT_HOST://0.0.0.0:10092
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LOG_DIRS: /tmp/kafka-combined-logs
      CLUSTER_ID: myCluster
  kafka-ui:
    image: provectuslabs/kafka-ui
    hostname: kafka-ui
    container_name: kafka-ui
    ports:
      - 10100:8080"
    restart: always
    environment:
      - KAFKA_CLUSTERS_0_NAME=myCluster
      - KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=kafka-broker:29092
  timescaledb:
    image: timescale/timescaledb:latest-pg16
    ports:
      - 10000:5432
    volumes:
      - /my_local_volumne/timescaledb-pgdata:/var/lib/postgresql/data
    environment:
      POSTGRES_DB: db_name
      POSTGRES_PASSWORD: my_db_user_password
      POSTGRES_USER: my_db_user
```

Once the docker-compose.yaml has been created, start the containers:

```
docker compose up -d
```

## Installation

First, create a python virtual environment:

```
python3 -m venv .venv-slurm-monitor
source .venv-slurm-monitor/bin/activate
```

Then clone the repo (currently the package is not available via pypi):
```
git clone https://github.com/2maz/slurm-monitor.git
```

Install locally with all dependencies (development and test included, refer to
pyproject.toml for the optional dependencies):

```
pip install slurm-monitor/[restapi,dev]
```


## Listener

Prepare the data collection to record published messages via:

```
slurm-monitor listen --host my-kafka-broker --db-uri timescaledb://my_db_user:my_db_user_password@db_hostname:10000/db_name
```

## REST Api:

To run the REST Api add an entry to the .env file with the desired database setup:

For sqlite:
```
    HOSTNAME=<kafka-brokers-hostname>
    SLURM_MONITOR_DATABASE_URI=sqlite:////tmp/slurm-monitor.db
```

For timescaledb:
```
    HOSTNAME=<kafka-brokers-hostname>
    SLURM_MONITOR_DATABASE_URI=timescaledb://my_db_user:my_db_user_password@db_hostname:10000/db_name
```


```bash
    python3 -m uvicorn --reload slurm_monitor.main:app --port 12000 --host 0.0.0.0
```

The API should now be accessible. Check the docs via:
```
    http://<your host>:12000/api/v1/docs
```

### Running with ssl

To run with ssl, get or create a certificate and run as follows:

```
    openssl req -nodes -new -x509 -keyout key.pem -out cert.pem -days 365 -subj "/C=countrycode/ST=state/L=city/O=Organization Name/OU=Unit Name/CN=cname/emailAddress=creator@yourdomain"
    python3 -m uvicorn --reload slurm_monitor.main:app --port 12000 --host 0.0.0.0 --ssl-keyfile key.pem --ssl-certfile cert.pem
```


## Probe

slurm-monitor relies on [sonar](https://github.com/NordicHPC/sonar) as a probe on the individual nodes.
As such it should be set up so that it communicates with the kafka broker that the above described listener will attach to.
Instruction for installation of sonar are found in the sonar repository - in addition to an example kafka configuratoin.


## Validation

Since slurm-monitor consumes sonar data, it has to align with [types communicated by sonar](https://raw.githubusercontent.com/NordicHPC/sonar/refs/heads/main/doc/types.spec.yaml).
In case you need to (re)generate the types.spec.yaml, please use the [documentation processor](https://github.com/NordicHPC/sonar/tree/main/util/process-doc) distributed with sonar.

### Spec vs. DB Schema Definitions

To check the degree of alignment of the database schema with sonar's types, you can either
check using the sonar type spec added to slurm-monitor:
```
slurm-monitor spec
```

or using an outside generated type spec via:

```
wget https://raw.githubusercontent.com/NordicHPC/sonar/refs/heads/main/doc/types.spec.yaml
slurm-monitor spec --spec-file types.spec.yaml
```

There is no complete one-to-one mapping from types to database schema, so that the current validation approach remains limited. Check the warnings.


### DB Schema Definitions vs. Actual DB State

When the database schema is updated, e.g., to align with the sonar spec, the actual state of the database might differ from the newly defind schema. 
To identify required changes of the database use:

```
slurm-monitor db --diff
```

The command connects by default to the database configured in the .env, otherwise provide a database uri, for instance:
```
slurm-monitor db --diff --db-uri timescaledb://my_db_user:my_db_user_password@db_hostname:10000/db_name
```

## Testing
For testing one can run tox.

```
    tox -e timescaledb
```

# License

Copyright (c) 2024-2025 Thomas Roehr, Simula Research Laboratory

This project is licensed under the terms of the [New BSD License](https://opensource.org/license/BSD-3-clause).
You are free to use, modify, and distribute this work, subject to the
conditions specified in the [LICENSE](./LICENSE) file.


# Acknowledgement

This work has been supported by the Norwegian Research Council through
the project [Norwegian Artificial Intelligence Cloud (NAIC)](https://www.naic.no/english/about/) (grant number: 322336).
