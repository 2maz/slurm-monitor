import re
import sys
from argparse import ArgumentParser

import pytest
import httpx
import os
import subprocess
import time

import slurm_monitor.cli.main as cli_main
from slurm_monitor.cli.db import DBParser
from slurm_monitor.cli.probe import ProbeParser
from slurm_monitor.cli.listen import ListenParser, ListenUiParser
from slurm_monitor.cli.system_info import SystemInfoParser
from slurm_monitor.cli.autodeploy import AutoDeployParser
from slurm_monitor.cli.query import QueryParser
from slurm_monitor.cli.restapi import RestapiParser
from slurm_monitor.cli.spec import SpecParser
from slurm_monitor.cli.data_import import ImportParser
from slurm_monitor.cli.test import TestParser

from slurm_monitor.db_operations import DBManager
from slurm_monitor.utils.command import Command
from slurm_monitor.db.v2.db_tables import SampleDisk
from slurm_monitor.app_settings import SLURM_MONITOR_RESTAPI_PORT

@pytest.fixture
def subparsers():
    return [
        "auto-deploy",
        "db",
        "import",
        "listen",
        "listen-ui",
        "probe",
        "query",
        "restapi",
        "spec",
        "system-info",
        "test"
    ]

def test_help(subparsers, capsys, monkeypatch):
    monkeypatch.setattr(sys, 'argv', ['slurm-monitor'])
    cli_main.run()
    captured = capsys.readouterr()

    for subparser in subparsers:
        assert re.search(subparser, captured.out), f"Help for subcommand '{subparser}' expected"


@pytest.mark.parametrize("name, klass", [
    [ "auto-deploy", AutoDeployParser ],
    [ "import", ImportParser ],
    [ "listen", ListenParser ],
    [ "listen-ui", ListenUiParser],
    [ "probe", ProbeParser ],
    [ "query", QueryParser ],
    [ "restapi", RestapiParser ],
    [ "system-info", SystemInfoParser ],
    [ "spec", SpecParser ],
    [ "db", DBParser ],
    [ "test", TestParser ],
])
def test_subparser(name, klass, script_runner):
    result = script_runner.run(['slurm-monitor', name, "--help"])
    assert result.returncode == 0, f"Expected --help option for {name} subparser"

    test_parser = ArgumentParser()
    klass(parser=test_parser)

    for a in test_parser._actions:
        if a.help == "==SUPPRESS==":
            continue

        for option in a.option_strings:
            assert re.search(option, result.stdout) is not None, f"Should have {option=}"

def test_spec(script_runner):
    result = script_runner.run(['slurm-monitor', 'spec'])
    assert re.search("implemented", result.stdout) is not None, "Implemented"

@pytest.mark.parametrize("timescaledb", [{'port': 7002, 'container-suffix': '-db_parser'}], indirect=["timescaledb"])
def test_db_parser(script_runner, timescaledb):
    cluster = "my-test-cluster"
    result = script_runner.run(['slurm-monitor', 'db', '--db-uri', timescaledb, "--insert-test-samples", cluster])
    assert result.returncode == 0
    cluster_result = Command.run("docker exec timescaledb-pytest-db_parser psql -U test test -tAq -c 'SELECT cluster from cluster_attributes'")

    cluster_entries = cluster_result.split("\n")
    assert len(cluster_entries) == 2

    unique_clusters = set(cluster_entries)
    assert len(unique_clusters) == 1

    assert list(unique_clusters)[0] == cluster

@pytest.mark.asyncio(loop_scope="function")
async def test_db_apply_changes(script_runner, test_db_v2, db_config, timescaledb):
    SampleDisk.__table__.drop(test_db_v2.engine)
    tablename = SampleDisk.__tablename__

    initial_status = DBManager.get_status(timescaledb)
    assert tablename not in initial_status

    result = script_runner.run(['slurm-monitor', 'db', "--db-uri", str(timescaledb), "--apply-changes"])
    assert result.returncode == 0
    assert re.search(r"added tables: \['" + tablename + r"'\]", result.stdout) is not None

    new_status = DBManager.get_status(timescaledb)
    assert tablename in new_status


@pytest.mark.asyncio(loop_scope="function")
async def test_restapi_env_file_not_existing(script_runner, test_db_v2, db_config, timescaledb):
    result = script_runner.run(['slurm-monitor', 'restapi', '--env-file', 'non-existing-envfile'])
    assert result.returncode != 0

@pytest.mark.asyncio(loop_scope="function")
async def test_restapi_env_file_via_args(script_runner, tmp_path, test_db_v2, db_config, timescaledb):
    """
    Use --env-file <filename> to point to the envfile which should be used
    """
    port = 55555
    with open(tmp_path / "existing-envfile", "w") as f:
        f.write(f"SLURM_MONITOR_DATABASE_URI={timescaledb}\n")
        f.write(f"SLURM_MONITOR_PORT={port}\n")

    p = subprocess.Popen(['slurm-monitor', 'restapi', '--env-file', str(tmp_path / 'existing-envfile')])

    time.sleep(5)
    response = httpx.get(f"http://localhost:{port}/api/v2/docs")

    p.kill()
    p.wait()

    assert response.status_code == 200

@pytest.mark.asyncio(loop_scope="function")
async def test_restapi_env_file_via_env(script_runner, tmp_path, test_db_v2, db_config, timescaledb):
    """
    Set the SLURM_MONITOR_ENV_FILE to point to the envfile which should be used
    """
    port = 55555
    with open(tmp_path / "existing-envfile", "w") as f:
        f.write(f"SLURM_MONITOR_DATABASE_URI={timescaledb}\n")
        f.write(f"SLURM_MONITOR_PORT={port}\n")

    env = os.environ.copy()
    env['SLURM_MONITOR_ENV_FILE'] = str(tmp_path / 'existing-envfile')
    p = subprocess.Popen(['slurm-monitor', 'restapi'], env=env)

    time.sleep(5)
    response = httpx.get(f"http://localhost:{port}/api/v2/docs")

    p.kill()
    p.wait()

    assert response.status_code == 200

@pytest.mark.asyncio(loop_scope="function")
async def test_restapi_env_file_with_overrides(script_runner, tmp_path, test_db_v2, db_config, timescaledb):
    """
    Using --env-file <filename> to point to the envfile which should be used, should take precedence over
    environment variables
    """
    port = 55554
    with open(tmp_path / ".a.env", "w") as f:
        f.write(f"SLURM_MONITOR_DATABASE_URI={timescaledb}\n")
        f.write(f"SLURM_MONITOR_PORT={port}\n")

    port = 55555
    with open(tmp_path / ".b.env", "w") as f:
        f.write(f"SLURM_MONITOR_DATABASE_URI={timescaledb}\n")
        f.write(f"SLURM_MONITOR_PORT={port}\n")

    env = os.environ.copy()
    env['SLURM_MONITOR_ENV_FILE'] = str(tmp_path / '.a.env')
    p = subprocess.Popen(['slurm-monitor', 'restapi', '--env-file', '.b.env'], env=env)

    time.sleep(5)
    response = httpx.get(f"http://localhost:{port}/api/v2/docs")

    p.kill()
    p.wait()

    assert response.status_code == 200

@pytest.mark.asyncio(loop_scope="function")
async def test_restapi_settings_from_env(script_runner, tmp_path, test_db_v2, db_config, timescaledb):
    """
    AppSettings should read setting plainly from env as well
    """
    env = os.environ.copy()
    env['SLURM_MONITOR_DATABASE_URI'] = timescaledb

    p = subprocess.Popen(['slurm-monitor', 'restapi'], env=env)

    time.sleep(5)
    response = httpx.get(f"http://localhost:{SLURM_MONITOR_RESTAPI_PORT}/api/v2/docs")

    p.kill()
    p.wait()

    assert response.status_code == 200
