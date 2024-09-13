import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from slurm_monitor.main import app

@pytest_asyncio.fixture(scope="module")
def client():
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

@pytest.mark.asyncio
async def test_monitor_nodes_info(client):
    response = client.get("/api/v1/monitor/nodes/info")
    assert response.status_code == 200

    json_data = response.json()
    assert "nodes" in json_data
