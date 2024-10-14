import pytest
import pytest_asyncio
from fastapi import HTTPException
from fastapi.testclient import TestClient
from slurm_monitor.main import app
from slurm_monitor.utils.slurm import Slurm
from slurm_monitor.api.v1.monitor import load_node_infos, validate_interval

from slurm_monitor.db.v1.db_tables import Nodes


@pytest_asyncio.fixture(scope="module")
def client(mock_slurm_command_hint, test_db_uri, monkeypatch_module):
    Slurm._BIN_HINTS = [ mock_slurm_command_hint ]
    monkeypatch_module.setenv("SLURM_MONITOR_DATABASE_URI", f"{test_db_uri}")

    with TestClient(app) as c:
        yield c

@pytest.mark.asyncio
async def test_metrics(client):
    response = client.get("/metrics")
    assert response.status_code == 200

@pytest.mark.asyncio
async def test_monitor_nodes(client):
    response = client.get("/api/v1/monitor/nodes")
    assert response.status_code == 200

    json_data = response.json()
    assert "nodes" in json_data
    nodenames = [x['name'] for x in json_data['nodes']]
    for i in range(1,23):
        assert f"n{i:03d}" in nodenames

@pytest.mark.asyncio
async def test_monitor_nodes_info(client, test_db):
    response = client.get("/api/v1/monitor/nodes/info")
    assert response.status_code == 200

    json_data = response.json()
    nodes = test_db.get_nodes()
    assert nodes
    for node in nodes:
        assert node in json_data["nodes"]

@pytest.mark.asyncio
async def test_job_system_status(client, test_db):
    response = client.get("/api/v1/monitor/jobs/1/system_status")
    assert response.status_code == 200

    json_data = response.json()
    assert "nodes" in json_data
    assert "node-1" in json_data["nodes"]
    assert "accumulated" in json_data["nodes"]["node-1"]

@pytest.mark.asyncio
async def test_job_info(client, test_db):
    response = client.get("/api/v1/monitor/jobs/1/info")
    assert response.status_code == 200

    json_data = response.json()
    assert "job_status" in json_data
    assert json_data["job_status"]["job_id"] == 1
    assert json_data["job_status"]["name"] == "test-job"

@pytest.mark.asyncio
async def test_nodes_gpu_status(client, test_db):
    response = client.get("/api/v1/monitor/nodes/node-0/gpu_status")
    assert response.status_code == 200

    json_data = response.json()
    assert "gpu_status" in json_data

@pytest.mark.asyncio
async def test_nodes_gpu_status_with_args(client, test_db):
    response = client.get("/api/v1/monitor/nodes/node-0/gpu_status")
    assert response.status_code == 200

    json_data = response.json()
    assert "gpu_status" in json_data



@pytest.mark.asyncio
async def test_nodes_refresh_info(client, test_db):
    response = client.get("/api/v1/monitor/nodes/refresh_info")
    assert response.status_code == 200

    json_data = response.json()
    nodes = test_db.get_nodes()
    assert nodes
    for node in nodes:
        assert node in json_data["nodes"]

def test_validate_interval():
    start_time_in_s = None
    end_time_in_s = None
    resolution_in_s = None

    start_time_in_s, end_time_in_s, resolution_in_s = validate_interval(start_time_in_s,
            end_time_in_s, resolution_in_s)

    # Checking defaults
    assert start_time_in_s is not None
    assert end_time_in_s is not None
    assert abs((end_time_in_s - start_time_in_s) - 60*60) <= 10E-3
    assert resolution_in_s == 60

    with pytest.raises(HTTPException, match="cannot be smaller"):
        validate_interval(100, 120, resolution_in_s)

    with pytest.raises(HTTPException, match="cannot exceed 14 days"):
        validate_interval(3600*24*14+101, 100, resolution_in_s)

    with pytest.raises(HTTPException, match="must be >= 1"):
        validate_interval(120, 100, -1)



def test_nodeinfo(client, test_db):
    load_infos = load_node_infos()
    nodes = test_db.get_nodes()
    assert nodes
    for node in nodes:
        assert node in load_infos
