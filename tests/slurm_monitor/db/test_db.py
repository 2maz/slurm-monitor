import pytest
from slurm_monitor.db.db import SlurmMonitorDB, DatabaseSettings
from slurm_monitor.db.db_tables import GPUStatus, Nodes
import datetime as dt
from pathlib import Path

import pandas

@pytest.fixture
def number_of_nodes() -> int:
    return 5

@pytest.fixture
def number_of_gpus() -> int:
    return 2

@pytest.fixture
def number_of_samples() -> int:
    return 50

@pytest.fixture
def test_db(tmp_path, number_of_nodes, number_of_gpus, number_of_samples) -> SlurmMonitorDB:
    db_path = Path(tmp_path) / "slurm-monitor.test.db"
    db_uri = f"sqlite:///{db_path.resolve()}"

    db_settings = DatabaseSettings(uri=db_uri)
    db = SlurmMonitorDB(db_settings)


    for i in range(0, number_of_nodes):
        nodename = f"node-{i}"
        db.insert_or_update(Nodes(name=nodename))

        for g in range(0, number_of_gpus):
            start_time = dt.datetime.now() - dt.timedelta(seconds=number_of_samples)
            for s in range(0, number_of_samples):
                sample = GPUStatus(
                        name="Tesla V100",
                        uuid=f"GPU-{nodename}:{g}",
                        local_id=g,
                        node=nodename,
                        power_draw=30,
                        temperature_gpu=30,
                        utilization_memory=10,
                        utilization_gpu=12,
                        memory_total=16*1024**3,
                        timestamp=start_time + dt.timedelta(seconds=s)
                )
                db.insert(sample)
    return db

def test_gpu_infos(test_db, number_of_nodes, number_of_gpus):
    for i in range(0, number_of_nodes):
        nodename = f"node-{i}"
        gpu_infos = test_db.get_gpu_infos(node=nodename)

        assert "gpus" in gpu_infos
        assert len(gpu_infos["gpus"]) == number_of_gpus

def test_gpu_status(test_db, number_of_nodes, number_of_gpus, number_of_samples):
    for i in range(0, number_of_nodes):
        nodename = f"node-{i}"

        resolution_in_s = 10
        gpu_status = test_db.get_gpu_status(node=nodename, resolution_in_s=resolution_in_s)
        assert len(gpu_status) >= (number_of_samples / resolution_in_s)

def test_dataframe(test_db):
    df = test_db._fetch_dataframe(GPUStatus, where=(GPUStatus.node == "node-1"))
    print(df)

