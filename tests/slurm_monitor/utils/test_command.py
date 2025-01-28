import pytest
from pathlib import Path
from slurm_monitor.utils.command import Command

def test_find(mock_slurm_command_hint):
    expected_path = Path(mock_slurm_command_hint) / "scontrol"
    assert str(Command.find(command="scontrol", hints=[ mock_slurm_command_hint ])) == str(expected_path)

    with pytest.raises(RuntimeError, match="could not find"):
        Command.find(command="no-scontrol", hints=[ mock_slurm_command_hint ])

def test_user():
    assert Command.get_user() is not None

def test_run():
    with pytest.raises(RuntimeError):
        Command.run("not-a-real-command")
