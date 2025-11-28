import pytest
import pytest_asyncio
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient
import re

from slurm_monitor.v2 import app
from slurm_monitor.utils.slurm import Slurm

v2_routes = []
for route in app.routes:
    if hasattr(route, "routes") and route.path.endswith("v2"):
        for api_route in route.routes:
            if type(api_route) is APIRoute:
                r = api_route.path

                if re.search("{query_name}", r):
                    continue

                r = r.replace("{cluster}","cluster-0")
                r = r.replace("{nodename}","cluster-0-node-0")
                r = r.replace("{partition}","cluster-0-partition-0")
                r = r.replace("{job_id}","1")
                r = r.replace("{epoch}","0")
                r = r.replace("{benchmark_name}","lambdal")


                m = re.search(r"[{}]",r)
                assert m is None, f"API Route should be expanded {r}"
                v2_routes.append(r)

@pytest_asyncio.fixture(loop_scope="module")
def client(mock_slurm_command_hint, test_db_v2, timescaledb, monkeypatch_module):
    Slurm._BIN_HINTS = [ mock_slurm_command_hint ]

    monkeypatch_module.setenv("SLURM_MONITOR_DATABASE_URI", f"{timescaledb}")
    monkeypatch_module.setenv("SLURM_MONITOR_JOBS_COLLECTOR", "false")

    with TestClient(app) as c:
        yield c

@pytest.mark.asyncio
async def test_metrics(client):
    response = client.get("/metrics")
    assert response.status_code == 200

@pytest.mark.asyncio
@pytest.mark.parametrize("endpoint",
     v2_routes
    )
async def test_ensure_response_from_all_endpoints(endpoint, client):
    client.get(f"/api/v2{endpoint}")
