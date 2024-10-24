# Slurm Monitor

This package provides mainly three components for monitoring GPU and CPU resource in (Slurm) cluster:

0. a probe that publishes monitoring data to a kafka broker
0. a listener that collects monitoring data a writes it to a database (sqlite or timeseriesdb)
0. a REST Api to make the data accessible, so that it can, for instance, be used with a [web frontend](https://github.com/2maz/slurm-monitor-frontend)

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

## Listener

Prepare the data collection to record published messages via:

```
slurm-monitor listen --host my-kafka-broker --db-uri sqlite:////tmp/my-db.sqlite
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

Now that the receiving end is in place, the probes can be started.
(Note, that the probes can also be started independently.)

In case you want to distribute the probe as a single binary, create a platform specific binary with:

```
tox -e build:nuitka
```

The binary is then generated as: dist/<arch>/slurm-monitor

Note, that there is no cross-compilation support at the moment. So the build is always targeting the architecture the program runs on.


One the binary has been installed on the target system run it with:

```
slurm-monitor probe --host my-kafka-broker
```

or to make it persist

```
nohup slurm-monitor probe --host my-kafka-broker > /tmp/slurm-monitor-probe.log 2>&1 &
```

Per default messages are published under under the 'node-status' topic. To change, e.g., for testing use '--publisher-topic node-status-test'



## Testing
For testing one can run tox.

For the sqlite3 version use
```
    tox
```

for testing against a timescaledb docker will be required:
```
    tox -e timescaledb
```

# License

Copyright (c) 2024 Thomas Roehr, Simula Research Laboratory

This project is licensed under the terms of the [New BSD License](https://opensource.org/license/BSD-3-clause).
You are free to use, modify, and distribute this work, subject to the
conditions specified in the LICENSE file.
