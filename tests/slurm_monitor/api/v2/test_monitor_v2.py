import pytest
import pytest_asyncio
import fastapi
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient
import re
from pathlib import Path
import json
import jwt
import copy

from slurm_monitor.v2 import app
from slurm_monitor.utils.slurm import Slurm
from slurm_monitor.db.v2.importer import DBJsonImporter
from slurm_monitor.app_settings import AppSettings

def parametrize_route(path,
                      cluster="cluster-0",
                      node="cluster-0-node-0"):
    if re.search("{query_name}", path):
        return path

    path = path.replace("{cluster}", cluster)
    path = path.replace("{nodename}", node)
    path = path.replace("{partition}","cluster-0-partition-0")
    path = path.replace("{job_id}","1")
    path = path.replace("{epoch}","0")
    path = path.replace("{benchmark_name}","lambdal")

    m = re.search(r"[{}]", path)
    if m is not None:
        raise RuntimeError(f"API Route should be expanded {path}")
    return path

def get_routes(identifier: str = "v2", **kwargs):
    routes = []
    for route in app.routes:
        if hasattr(route, "routes") and route.path.endswith(identifier):
            for api_route in route.routes:
                if type(api_route) is APIRoute:
                    r = api_route.path
                    r = parametrize_route(r, **kwargs)

                    routes.append(r)
    return routes

v2_routes = get_routes(identifier="v2")

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


@pytest.mark.asyncio
@pytest.mark.parametrize("endpoint, cluster, node, sonar_msg_files, expected_exception, has_sysinfo",
    [
        [
            # no cluster or sysinfo messages, but only samples arrived for the node being requested
            "/api/v2/cluster/{cluster}/nodes/{nodename}/info",
            "ex3.simula.no",
            "g001",
            [
                "0+sample-g001.ex3.simula.no.json",
                "0+sample-ml1.hpc.uio.no.json",
            ],
            True,
            False,
        ],
        [
            # no cluster or sysinfo messages for some nodes in other clusters, but available for the node being requested
            "/api/v2/cluster/{cluster}/nodes/{nodename}/info",
            "cluster-0",
            "cluster-0-node-0",
            [
                "0+sample-g001.ex3.simula.no.json",
                "0+sample-ml1.hpc.uio.no.json",
            ],
            False,
            True,
        ],
        [
            # no cluster or sysinfo messages for the node which is requested
            "/api/v2/cluster/{cluster}/nodes/{nodename}/info",
            "cluster-0",
            "cluster-0-node-X",
            [
                "0+sample-node-X.cluster-0.json",
            ],
            False,
            False,
        ],
    ])
async def test_ensure_response_with_partial_rows(endpoint,
                                                 cluster, node,
                                                 sonar_msg_files,
                                                 expected_exception,
                                                 has_sysinfo,
                                                 client,
                                                 test_db_v2__function_scope,
                                                 test_data_dir,
                                                 monkeypatch,
                                                ):
    db = test_db_v2__function_scope
    importer = DBJsonImporter(db=db)

    payload = {
        'exp': 1765187642,
        'iat': 1765187342,
        'jti': 'onrtro:e0070d78-9c5c-04ba-22b9-fbcecb759fe4',
        'iss': 'http://158.39.75.110/realms/naic-monitor',
        'aud': 'account',
        'sub': 'fbf4b6c4-bdd3-4aea-8b47-3a87e9c96633',
        'typ': 'Bearer',
        'azp': 'slurm-monitor.no',
        'sid': '0e7aad19-8b15-06c2-1449-616b92625b9b',
        'acr': 1,
        'allowed-origins': ['', 'https://naic-monitor.simula.no'],
        'realm_access' : { 'roles' : ['default-roles-naic-monitor', 'offline_access', 'uma_authorization'] },
        'resource_access': { 'account' : { 'roles': ['manage-account', 'manage-account-links', 'view-profile']}},
        'scope': 'email profile',
        'email_verified': False,
        'name': 'Test User',
        'preferred_username': 'test-user',
        'given_name': 'Test',
        'family_name': 'User',
        'email': 'test-user@xyz.com'
    }

    test_token = "oauth-test-token"
    def patch_decode(*args, **kwargs):
        token = args[0]
        if not token == test_token:
            raise jwt.InvalidTokenError(f"The expected token is {test_token}, but was {token}")

        return payload

    def patch_get_signing_key_from_jwt(token):
        class SigningKey:
            key: str = "signing_key"

        return SigningKey()

    monkeypatch.setattr(jwt, "decode", patch_decode)

    app_settings = AppSettings.get_instance()
    monkeypatch.setattr(app_settings.oauth.jwks_client, "get_signing_key_from_jwt", patch_get_signing_key_from_jwt)

    for sonar_msg_file in sonar_msg_files:
        json_filename = Path(test_data_dir) / "sonar" / sonar_msg_file
        with open(json_filename, "r") as f:
            msg_data = json.load(f)
            await importer.insert(copy.deepcopy(msg_data))

    route = parametrize_route(endpoint, cluster=cluster, node=node)
    try:
        response = client.get(f"{route}",
                              headers={"Authorization": f"Bearer {test_token}"}
                   )
        assert not expected_exception, "Exception exception for '{route}', but was not raised"

        data = response.json()
        if has_sysinfo:
            assert len(data) == 1 and node in data
            assert data[node]['cluster'] == cluster
            assert data[node]['node'] == node
            assert data[node]['cards']
        else:
            assert data == {}, f"{node=} is expected to have no system information, but was {data}"
    except fastapi.exceptions.HTTPException:
        assert expected_exception
