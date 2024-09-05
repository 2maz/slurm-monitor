import pytest
from slurm_monitor.db.db import SlurmMonitorDB, DatabaseSettings
from slurm_monitor.db.db_tables import GPUStatus, Nodes
import datetime as dt
from pathlib import Path

@pytest.fixture
def number_of_nodes() -> int:
    return 5

@pytest.fixture
def number_of_gpus() -> int:
    return 2

@pytest.fixture
def test_db(tmp_path, number_of_nodes, number_of_gpus) -> SlurmMonitorDB:
    db_path = Path(tmp_path) / "slurm-monitor.test.db"
    db_uri = f"sqlite:///{db_path.resolve()}"

    db_settings = DatabaseSettings(uri=db_uri)
    db = SlurmMonitorDB(db_settings)


    for i in range(0, number_of_nodes):
        nodename = f"node-{i}"
        db.insert_or_update(Nodes(name=nodename))

        for g in range(0, number_of_gpus):
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
                    timestamp=dt.datetime.now()
            )

            db.insert(sample)
    return db

def test_gpu_infos(test_db, number_of_nodes, number_of_gpus):
    for i in range(0, number_of_nodes):
        nodename = f"node-{i}"
        gpu_infos = test_db.get_gpu_infos(node=nodename)

        assert "gpus" in gpu_infos
        assert len(gpu_infos["gpus"]) == number_of_gpus
