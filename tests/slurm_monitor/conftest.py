import pytest
from slurm_monitor.db.v1.db import SlurmMonitorDB, DatabaseSettings
from slurm_monitor.db.v1.db_tables import GPUs, GPUStatus, JobStatus, Nodes, ProcessStatus
import datetime as dt
from pathlib import Path
from slurm_monitor.utils import utcnow

def pytest_addoption(parser):
    parser.addoption("--db-uri", help="Test database ", default=None)

@pytest.fixture(scope='module')
def monkeypatch_module():
    with pytest.MonkeyPatch.context() as mp:
        yield mp

@pytest.fixture(scope="session")
def mock_slurm_command_hint():
    return Path(__file__).parent.parent / "mock"

@pytest.fixture(scope='module')
def number_of_nodes() -> int:
    return 5

@pytest.fixture(scope='module')
def number_of_gpus() -> int:
    return 2

@pytest.fixture(scope='module')
def number_of_samples() -> int:
    return 50

@pytest.fixture(scope="module")
def test_db_uri(tmp_path_factory, pytestconfig):
    #print("test_db_uri: fixture start")
    db_uri = pytestconfig.getoption("db_uri")
    if not db_uri:
        path = tmp_path_factory.mktemp("data") / "slurm-monitor.test.sqlite"
        return f"sqlite:///{path}"
    return db_uri

@pytest.fixture(scope="module")
def test_db(test_db_uri, number_of_nodes, number_of_gpus, number_of_samples) -> SlurmMonitorDB:
    db_settings = DatabaseSettings(uri=test_db_uri)
    db = SlurmMonitorDB(db_settings)

    db.clear()


    for i in range(0, number_of_nodes):
        nodename = f"node-{i}"
        db.insert_or_update(Nodes(name=nodename, cpu_count=128))


        for g in range(0, number_of_gpus):
            start_time = dt.datetime.now() - dt.timedelta(seconds=number_of_samples)
            for s in range(0, number_of_samples):
                uuid=f"GPU-{nodename}:{g}"
                db.insert_or_update(GPUs(
                    uuid=uuid,
                    model="Tesla V100",
                    local_id=g,
                    node=nodename,
                    memory_total=16*1024**3
                ))
                sample = GPUStatus(
                        uuid=uuid,
                        power_draw=30,
                        temperature_gpu=30,
                        utilization_memory=10,
                        utilization_gpu=12,
                        timestamp=start_time + dt.timedelta(seconds=s)
                )
                db.insert(sample)


        job_id = i
        sample_count = 100

        end_time = utcnow()
        start_time = end_time - dt.timedelta(seconds=sample_count)
        submit_time = start_time - dt.timedelta(seconds=20)
        db.insert(JobStatus(
            job_id=job_id,
            submit_time=submit_time,
            name="test-job",
            start_time= start_time,
            end_time=end_time,
            account="account",
            accrue_time=10000,
            admin_comment="",
            array_job_id=1,
            array_task_id=1,
            array_max_tasks=20,
            array_task_string="",
            association_id=1,
            batch_host=nodename,
            cluster="slurm",
            derived_exit_code=0,
            eligible_time=1000,
            exit_code=0,
            gres_detail=[],
            group_id=0,
            job_state="COMPLETED",
            nodes=nodename,
            cpus=1,
            node_count=1,
            tasks=1,
            partition="slowq",
            state_description="",
            state_reason="",
            suspend_time=0,
            time_limit=7200,
            user_id=1000,
            )
        )

        for pid in range(1, 10):
            samples = []
            for idx in range(1, sample_count+1):
                timestamp = end_time - dt.timedelta(seconds=idx)
                samples.append(ProcessStatus(
                        pid=pid, node=nodename,
                        job_id=job_id, job_submit_time=submit_time,
                        cpu_percent=0.5, memory_percent=0.2,
                        timestamp=timestamp
                        )
                )
            samples.reverse()
            db.insert(samples)
    return db
