import pytest
import os
from slurm_monitor.utils.slurm import Slurm

def test_ensure_commands(mock_slurm_command_hint):
    Slurm._BIN_HINTS = [ mock_slurm_command_hint ]
    Slurm.ensure_commands()

def test_ensure_raises(mock_slurm_command_hint, monkeypatch):
    Slurm._BIN_HINTS = []
    monkeypatch.setenv("PATH", "")
    with pytest.raises(RuntimeError):
        Slurm.ensure_commands()


def test_get_slurmrestd(mock_slurm_command_hint):
    Slurm._BIN_HINTS = [ mock_slurm_command_hint ]

    data = Slurm.get_slurmrestd("nodes")
    assert "nodes" in data
    assert len(data["nodes"]) > 0


def test_get_node_names(mock_slurm_command_hint):
    Slurm._BIN_HINTS = [ mock_slurm_command_hint ]

    names = Slurm.get_node_names()
    assert len(names) > 0
    for i in range(1,23):
        assert f"n{i:03d}" in names
