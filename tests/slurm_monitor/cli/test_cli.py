import re
import sys
from argparse import ArgumentParser
from pathlib import Path

import pytest
import yaml
from pytest_console_scripts import ScriptRunner

import slurm_monitor.cli.main as cli_main
from slurm_monitor.cli.spec import SpecParser
from slurm_monitor.cli.db import DBParser
from slurm_monitor.cli.probe import ProbeParser
from slurm_monitor.cli.listen import ListenParser
from slurm_monitor.cli.system_info import SystemInfoParser
from slurm_monitor.cli.autodeploy import AutoDeployParser
from slurm_monitor.cli.query import QueryParser
from slurm_monitor.cli.spec import SpecParser
from slurm_monitor.cli.data_import import ImportParser
from slurm_monitor.cli.test import TestParser


@pytest.fixture
def subparsers():
    return [
        "auto-deploy",
        "db",
        "import",
        "listen",
        "probe",
        "query",
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
    [ "spec", SpecParser ],
])
def test_subparser(name, klass, script_runner):
    result = script_runner.run(['slurm-monitor', name, "--help"])
    assert result.returncode == 0

    test_parser = ArgumentParser()
    subparser = klass(parser=test_parser)

    for a in test_parser._actions:
        if a.help == "==SUPPRESS==":
            continue

        for option in a.option_strings:
            assert re.search(option, result.stdout) is not None, f"Should have {option=}"

def test_spec(script_runner):
    result = script_runner.run(['slurm-monitor', 'spec'])
    assert re.search("implemented", result.stdout) is not None, "Implemented"
