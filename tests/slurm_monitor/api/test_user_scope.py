"""
Routes that attribute data to jobs or users show a user only their own;
an admin (realm role 'admin') sees everyone's, as with the jobs routes.
"""

from types import SimpleNamespace

import jwt
import pytest
import pytest_asyncio
from fastapi.testclient import TestClient

from slurm_monitor.app import app
from slurm_monitor.app_settings import AppSettings
from slurm_monitor.db.db_base import DatabaseSettings
from slurm_monitor.db.db_testing import TestDBConfig
from slurm_monitor.db_operations import DBManager
from slurm_monitor.utils.slurm import Slurm

USER = "user-a"
OTHER = "user-b"
ADMIN = "admin-user"
CLUSTER = "cluster-0"
NODE = "cluster-0-node-0"

# create_test_db hands the jobs out to the users in turn: on cluster-0,
# node-0 runs jobs 1-4 and node-1 jobs 9-12, and the odd ones are USER's.
# A job's processes have its id as pid.
NODE_JOBS = {1, 2, 3, 4}
CLUSTER_JOBS = {1, 2, 3, 4, 9, 10, 11, 12}


def own(jobs: set[int]) -> set[int]:
    return {job for job in jobs if job % 2}


@pytest.fixture(scope="module")
def db_config() -> TestDBConfig:
    return TestDBConfig(user_names=[USER, OTHER])


@pytest_asyncio.fixture(loop_scope="module", scope="module")
def client(mock_slurm_command_hint, test_db, timescaledb, monkeypatch_module):
    Slurm._BIN_HINTS = [mock_slurm_command_hint]

    monkeypatch_module.setenv("SLURM_MONITOR_DATABASE_URI", f"{timescaledb}")
    monkeypatch_module.setenv("SLURM_MONITOR_JOBS_COLLECTOR", "false")
    monkeypatch_module.setenv("SLURM_MONITOR_PREFETCH_ENABLED", "false")
    # a database whose connection pool belongs to this client's event loop,
    # and no responses cached by an earlier module
    monkeypatch_module.setattr(DBManager, "_databases", None)

    with TestClient(app) as c:
        assert c.get("/api/v2/clear_cache").status_code == 200
        yield c


def payload(username: str, roles: list[str]) -> dict:
    return {
        "exp": 1765187642,
        "iat": 1765187342,
        "jti": f"jti-{username}",
        "iss": "http://myidentityprovider.com/realms/naic-monitor",
        "aud": "account",
        "sub": f"sub-{username}",
        "typ": "Bearer",
        "azp": "slurm-monitor.no",
        "sid": f"sid-{username}",
        "acr": 1,
        "realm_access": {"roles": roles},
        "resource_access": {"account": {"roles": []}},
        "scope": "email profile",
        "email_verified": False,
        "name": username,
        "preferred_username": username,
        "given_name": username,
        "family_name": username,
        "email": f"{username}@xyz.com",
    }


@pytest.fixture
def headers(monkeypatch, timescaledb) -> dict[str, dict[str, str]]:
    """Request headers of a user and of an admin, with OAuth required."""
    app_settings = AppSettings.get_instance()
    monkeypatch.setattr(app_settings.oauth, "required", True)
    monkeypatch.setattr(app_settings.oauth, "url", "https://example.invalid")
    monkeypatch.setattr(app_settings.oauth, "realm", "test-realm")
    monkeypatch.setattr(app_settings, "database", DatabaseSettings(uri=timescaledb))

    payloads = {
        "user-token": payload(USER, ["default-roles-naic-monitor"]),
        "admin-token": payload(ADMIN, ["default-roles-naic-monitor", "admin"]),
    }

    def decode(token, *args, **kwargs):
        if token not in payloads:
            raise jwt.InvalidTokenError(f"unexpected token {token}")
        return payloads[token]

    monkeypatch.setattr(jwt, "decode", decode)
    monkeypatch.setattr(
        app_settings.oauth.jwks_client,
        "get_signing_key_from_jwt",
        lambda token: SimpleNamespace(key="signing-key"),
    )
    return {
        "user": {"Authorization": "Bearer user-token"},
        "admin": {"Authorization": "Bearer admin-token"},
    }


def get(client, path: str, headers: dict[str, str]):
    response = client.get(f"/api/v2{path}", headers=headers)
    assert response.status_code == 200, response.text
    return response.json()


def partition_users(partitions: list[dict]) -> set[str]:
    return {job["user_name"] for p in partitions for job in p["jobs_pending"] + p["jobs_running"]}


def test_partitions_list_only_the_callers_jobs(client, headers):
    path = f"/cluster/{CLUSTER}/partitions"
    # the admin's response is cached first, for 120 s
    admin = get(client, path, headers["admin"])
    assert partition_users(admin) == {USER, OTHER}

    user = get(client, path, headers["user"])
    assert partition_users(user) == {USER}
    # counts without attribution still cover every job
    assert [p["total_gpus"] for p in user] == [p["total_gpus"] for p in admin]
    assert [p["gpus_reserved"] for p in user] == [p["gpus_reserved"] for p in admin]


def process_gpu_jobs(nodes: dict) -> set[int]:
    return {series["job"] for node in nodes.values() for per_gpu in node["gpus"].values() for series in per_gpu}


@pytest.mark.parametrize(
    "path,jobs",
    [
        (f"/cluster/{CLUSTER}/nodes/{NODE}/process/gpu/timeseries", NODE_JOBS),
        (f"/cluster/{CLUSTER}/nodes/process/gpu/timeseries", CLUSTER_JOBS),
    ],
)
def test_process_gpu_timeseries_only_of_the_callers_jobs(client, headers, path, jobs):
    assert process_gpu_jobs(get(client, path, headers["admin"])) == jobs
    assert process_gpu_jobs(get(client, path, headers["user"])) == own(jobs)


def test_process_gpu_timeseries_of_another_users_job_is_empty(client, headers):
    path = f"/cluster/{CLUSTER}/nodes/{NODE}/jobs/2/process/gpu/timeseries"
    assert process_gpu_jobs(get(client, path, headers["admin"])) == {2}
    assert process_gpu_jobs(get(client, path, headers["user"])) == set()


def test_process_gpu_util_sums_only_the_callers_processes(client, headers):
    path = f"/cluster/{CLUSTER}/nodes/{NODE}/process/gpu/util"
    admin = get(client, path, headers["admin"])[NODE]
    user = get(client, path, headers["user"])[NODE]

    assert admin and set(user) == set(admin)
    for uuid, card in admin.items():
        assert set(card["pids"]) == NODE_JOBS
        assert set(user[uuid]["pids"]) == own(NODE_JOBS)
        assert user[uuid]["gpu_util"] < card["gpu_util"]


@pytest.mark.parametrize(
    "query",
    [
        "user-job-results",
        "user-success-job-results",
        "user-failed-job-results",
        "jobs-exceeding-resource-usage",
    ],
)
def test_queries_listing_users_or_their_jobs_are_for_admins(client, headers, query):
    path = f"/api/v2/cluster/{CLUSTER}/queries/{query}"
    # The route caches by URL alone, so an admin's result is there first.
    # The user-success/failed queries take no cluster parameter yet and
    # answer 422 to anyone the check lets through.
    assert client.get(path, headers=headers["admin"]).status_code in (200, 422)
    assert client.get(path, headers=headers["user"]).status_code == 403


def test_aggregate_queries_stay_open(client, headers):
    get(client, f"/cluster/{CLUSTER}/queries/popular-partitions-by-number-of-jobs", headers["user"])


def test_jobs_are_cached_per_user(client, headers):
    path = f"/cluster/{CLUSTER}/jobs"
    admin = get(client, path, headers["admin"])["jobs"]
    assert {job["user_name"] for job in admin} == {USER, OTHER}

    user = get(client, path, headers["user"])["jobs"]
    assert {job["user_name"] for job in user} == {USER}


def test_card_metrics_stay_open(client, headers):
    assert get(client, f"/cluster/{CLUSTER}/nodes/{NODE}/gpu/timeseries", headers["user"])[NODE]
