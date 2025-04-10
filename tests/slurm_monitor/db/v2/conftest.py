import pytest
import json
import subprocess
import psutil

import slurm_monitor.timescaledb
from slurm_monitor.db.v2.db import ClusterDB, DatabaseSettings
from slurm_monitor.db.v2.db_tables import (
        Cluster,
        Partition,
        Node,
        NodeState,
        SampleGpu,
        SampleProcess,
        SampleProcessGpu,
        SampleSlurmJob,
        SampleSlurmJobAcc,
        SysinfoAttributes,
        SysinfoGpuCard,
        SysinfoGpuCardConfig,
)
import datetime as dt
from pathlib import Path
from slurm_monitor.utils import utcnow

from slurm_monitor.devices.gpu import GPU
from slurm_monitor.utils.command import Command
from time import sleep

@pytest.fixture(scope='module')
def monkeypatch_module():
    with pytest.MonkeyPatch.context() as mp:
        yield mp

@pytest.fixture(scope='module')
def number_of_clusters() -> int:
    return 2

@pytest.fixture(scope='module')
def number_of_partitions() -> int:
    return 2

@pytest.fixture(scope='module')
def number_of_nodes() -> int:
    return 2

@pytest.fixture(scope='module')
def number_of_gpus() -> int:
    return 2

@pytest.fixture(scope='module')
def number_of_cpus() -> int:
    return 2

@pytest.fixture(scope='module')
def number_of_samples() -> int:
    return 25

@pytest.fixture(scope='module')
def number_of_jobs() -> int:
    return 4


@pytest.fixture(scope="module")
def timescaledb(request):
    container_name = "timescaledb-pytest"

    container = Command.run(f"docker ps -f name={container_name} -q")
    if container != "":
        Command.run(f"docker stop {container_name}")

    Command.run(f"docker run -d --rm --name {container_name} -p 7001:5432 -e POSTGRES_DB=test -e POSTGRES_PASSWORD=test -e POSTGRES_USER=test timescale/timescaledb:latest-pg17")

    for i in range(0, 3):
        sleep(2)
        container = Command.run(f"docker ps -f name={container_name} -q")
        if container:
            break

    sleep(3)
    print(f"{container_name=} is ready")

    def teardown():
        Command.run(f"docker stop {container_name}")

    request.addfinalizer(teardown)


    return "timescaledb://test:test@localhost:7001/test"


@pytest.fixture(scope="module")
def test_db_v2(timescaledb,
        number_of_clusters, number_of_partitions, number_of_nodes,
        number_of_cpus, number_of_gpus,
        number_of_jobs, number_of_samples) -> ClusterDB:
    db_settings = DatabaseSettings(uri=timescaledb)
    dbi = ClusterDB(db_settings)

    dbi.clear()

    time = utcnow()
    virtual_memory = psutil.virtual_memory()._asdict()

    for c in range(0, number_of_clusters):
        cluster_name = f"cluster-{c}"

        partitions = [ f"cluster-{c}-partition-{p}" for p in range(0, number_of_partitions) ]
        nodes = [ f"cluster-{c}-node-{n}" for n in range(0, number_of_nodes) ]

        dbi.insert(Cluster(
                cluster=cluster_name,
                slurm=True,
                partitions=partitions,
                nodes=nodes,
                time=time
            )
        )

        for p in partitions:
            dbi.insert(
                Partition(
                    cluster=cluster_name,
                    partition=p,
                    # just add all nodes
                    nodes=nodes,
                    nodes_compact=[f"cluster-{c}-node-[0,{number_of_nodes-1}]"],
                    time=time
            )
        )

        for nIdx, n in enumerate(nodes):
            dbi.insert(Node(
                    cluster=cluster_name,
                    node=n,
                    architecture="x86_64"
                )
            )

            dbi.insert(
                NodeState(
                    cluster=cluster_name,
                    node=n,
                    states=["IDLE"],
                    time=time
                )
            )

            cards = []
            for gpu in range(0, number_of_gpus):
                uuid = f"gpu-uuid-{n}-{gpu}"
                dbi.insert(SysinfoGpuCard(
                        uuid=uuid,
                        manufacturer="NVIDIA",
                        model="Tesla V100-SMX3-32GB",
                        architecture="Volta",
                        memory="33554432",
                    )
                )

                dbi.insert(SysinfoGpuCardConfig(
                        cluster=cluster_name,
                        node=n,
                        uuid=uuid,
                        index=0,
                        address=f"00000000:34:00.{gpu}",
                        firmware="12.2",
                        driver="535.230.02",
                        power_limit=350,
                        min_power_limit=100,
                        max_power_limit=350,
                        max_ce_clock=1597,
                        max_memory_clock=958,
                        time=time,
                    )
                )
                cards.append(uuid)

                sampling_interval=30
                start_time = time - dt.timedelta(seconds=number_of_samples*sampling_interval)
                sample_time = start_time
                for s in range(0, number_of_samples):
                    dbi.insert(
                        SampleGpu(
                            uuid=uuid,
                            index=gpu,
                            failing=0,
                            fan=100,
                            compute_mode="P",
                            performance_state=1,
                            memory=278656,
                            ce_util=30,
                            memory_util=20,
                            temperature=52,
                            power=41,
                            power_limit=350,
                            ce_clock=135,
                            memory_clock=958,
                            time=sample_time
                        )
                    )
                    sample_time += dt.timedelta(seconds=sampling_interval)


            dbi.insert(
                SysinfoAttributes(
                    time=time,
                    cluster=cluster_name,
                    node=n,
                    os_name="Linux",
                    os_release="Ubuntu 24.04",
                    architecture="x86_64",
                    sockets=2,
                    cores_per_socket=24,
                    threads_per_core=2,
                    cpu_model="Intel Xeon",
                    description=f"This is {n}",
                    memory=256*1024**2,
                    topo_svg=None,
                    cards=cards

                )
            )

            for j in range(1, number_of_jobs+1):
                jobId = nIdx*len(nodes)*number_of_jobs + j
                sampling_interval=30
                start_time = time - dt.timedelta(seconds=number_of_samples*sampling_interval)
                sample_time = start_time
                for s in range(0, number_of_samples):
                    dbi.insert(
                            SampleSlurmJob(
                                cluster=cluster_name,
                                job_id=jobId,
                                job_step="",
                                job_name=f"job-{jobId}-on-{n}",
                                job_state="RUNNING",
                                array_job_id=None,
                                array_task_id=None,

                                het_job_id=0,
                                het_job_offset=0,
                                user_name="any-user",
                                account="any-account",

                                start_time=start_time,
                                suspend_time=0,
                                submit_time=start_time - dt.timedelta(seconds=100*60),
                                time_limit=604801,
                                end_time=None,
                                exit_code=None,

                                partition=f"cluster-{c}-partition-0",
                                reservation="",
                                nodes=[n],
                                priority=1,
                                distribution="",
                                gres_detail=None,
                                requested_cpus = 10,
                                requested_memory_per_node = 10*1024**2,
                                requested_node_count=1,
                                minimum_cpus_per_node=0,
                                time=sample_time
                            )
                    )
                    dbi.insert(
                            SampleSlurmJobAcc(
                                cluster=cluster_name,
                                job_id=jobId,
                                job_step="",
                                AllocTRES="cpu=5,gres/gpu=1,mem=0,node=1",
                                ElapsedRaw=number_of_samples*sampling_interval,
                                AveRSS=25863946240,
                                AveVMSize=119095128064,
                                MaxRSS=25863946240,
                                MaxVMSize=119095128064,
                                MinCPU=15692,
                                time=sample_time
                            )
                    )
                    dbi.insert(SampleProcess(
                            cluster=cluster_name,
                            node=n,
                            job=jobId,
                            epoch=0,
                            user="any-user",
                            resident_memory=8*1024*2,
                            virtual_memory=2*1024*2,
                            cmd='test-command',
                            pid=jobId,
                            ppid=0,
                            cpu_avg=20,
                            cpu_util=70,
                            cpu_time=10*s,
                            time=sample_time
                        )
                    )

                    for cIdx, card in enumerate(cards):
                        dbi.insert(
                            SampleProcessGpu(
                                cluster=cluster_name,
                                node=n,
                                job=jobId,
                                epoch=0,
                                user="any-user",
                                pid=jobId,
                                uuid=card,
                                gpu_util=30,
                                gpu_memory=80*32*1024**2,
                                gpu_memory_util=80,
                                time=sample_time
                            )
                        )
                    sample_time += dt.timedelta(seconds=sampling_interval)

    return dbi
