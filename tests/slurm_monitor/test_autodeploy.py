from slurm_monitor.autodeploy import AutoDeployer
from slurm_monitor.app_settings import AppSettings
from slurm_monitor.db import DatabaseSettings
import time

def test_autodeployer(test_db, test_db_uri, number_of_nodes, monkeypatch):

    redeploy_nodes = []
    def mock_deploy(self, node):
        redeploy_nodes.append(node)

    app_settings = AppSettings()
    app_settings.db_schema_version = "v1"
    app_settings.database = DatabaseSettings(
            uri=test_db_uri,
    )

    monkeypatch.setattr(AutoDeployer, "deploy", mock_deploy)
    monkeypatch.setattr(AutoDeployer, "is_drained", lambda x,node: True if node == "node-1" else False)

    auto_deployer = AutoDeployer(app_settings=app_settings, sampling_interval_in_s=1)
    auto_deployer.start()

    time.sleep(3)
    auto_deployer.stop()

    assert len(set(redeploy_nodes)) == number_of_nodes - 1
    assert "node-1" not in redeploy_nodes
