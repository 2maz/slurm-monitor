from slurm_monitor.autodeploy import AutoDeployer, AutoDeployerSonar
from slurm_monitor.app_settings import AppSettings
from slurm_monitor.db import DatabaseSettings
import time

def test_AutoDeployer_v1(test_db, test_db_uri, number_of_nodes, monkeypatch):
    redeploy_nodes = set()

    def mock_deploy(self, node):
        redeploy_nodes.add(node)

    # exclude drained nodes from redeployment
    async def mock_is_drained(self, node):
        return True if node == "node-0" else False

    def mock_all_nodes(self) -> list[str]:
        return [f"node-{x}" for x in range(0, number_of_nodes)]

    app_settings = AppSettings()
    app_settings.db_schema_version = "v1"
    app_settings.database = DatabaseSettings(
            uri=test_db_uri,
    )

    monkeypatch.setattr(AutoDeployer, "deploy", mock_deploy)
    monkeypatch.setattr(AutoDeployer, "is_drained", mock_is_drained)
    monkeypatch.setattr(AutoDeployer, "all_nodes", mock_all_nodes)

    auto_deployer = AutoDeployer(app_settings=app_settings, sampling_interval_in_s=1)
    auto_deployer.start()

    time.sleep(3)
    auto_deployer.stop()

    assert len(set(redeploy_nodes)) == number_of_nodes - 1
    assert "node-0" not in redeploy_nodes

def test_AutoDeployer_v2(timescaledb, test_db_v2, db_config, monkeypatch):
    redeploy_nodes = set()

    def mock_deploy(self, node):
        redeploy_nodes.add(node)

    # exclude drained nodes from redeployment
    async def mock_is_drained(self, node):
        return True if node == "cluster-0-node-0" else False

    def mock_all_nodes(self) -> list[str]:
        return [f"cluster-0-node-{x}" for x in range(0, db_config.number_of_nodes)]

    app_settings = AppSettings()
    app_settings.db_schema_version = "v2"
    app_settings.database = DatabaseSettings(
            uri=timescaledb
    )

    monkeypatch.setattr(AutoDeployerSonar, "deploy", mock_deploy)
    monkeypatch.setattr(AutoDeployerSonar, "is_drained", mock_is_drained)
    monkeypatch.setattr(AutoDeployerSonar, "all_nodes", mock_all_nodes)

    auto_deployer = AutoDeployerSonar(
            app_settings=app_settings,
            sampling_interval_in_s=1,
            cluster_name="cluster-0")
    auto_deployer.start()

    time.sleep(3)
    auto_deployer.stop()

    assert len(redeploy_nodes) == db_config.number_of_nodes - 1, f"Trying to redeploy {redeploy_nodes}, but expected only {db_config.number_of_nodes - 1} nodes, due to one drained node"
    assert "cluster-0-node-0" not in redeploy_nodes
