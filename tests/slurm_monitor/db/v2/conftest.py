import pytest
import slurm_monitor.timescaledb
from slurm_monitor.db.v2.db import ClusterDB, DatabaseSettings
from slurm_monitor.utils.command import Command

from slurm_monitor.db.v2.db_testing import (
    create_test_db,
    start_timescaledb_container,
    TestDBConfig
)

@pytest.fixture(scope='module')
def monkeypatch_module():
    with pytest.MonkeyPatch.context() as mp:
        yield mp

@pytest.fixture(scope='module')
def db_config() -> TestDBConfig:
    return TestDBConfig()

@pytest.fixture(scope="module")
def timescaledb(request):
    container_name = "timescaledb-pytest"
    uri = start_timescaledb_container(
            port=7001,
            container_name=container_name
    )

    def teardown():
        Command.run(f"docker stop {container_name}")

    request.addfinalizer(teardown)
    return uri

@pytest.fixture(scope="module")
def test_db_v2(timescaledb,
        db_config) -> ClusterDB:

    return create_test_db(timescaledb, db_config)
