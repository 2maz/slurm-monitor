import pytest
from slurm_monitor.utils.slurm import Slurm

from slurm_monitor.api.v2.response_models import AllocTRES

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


@pytest.mark.parametrize("txt,expected",
    [
        ["billing=12,cpu=6,gres/gpu=1,mem=48G,node=1", { 'billing': 12, 'cpu': 6, 'gpu': 1, 'mem': 48*1024**3, 'node': 1 }],
        ["billing=20.3,cpu=20,gres/gpu:rtx30=1,gres/gpu=1,mem=50000M,node=1", {'billing': 20.3, 'cpu': 20, 'gpu:rtx30': 1, 'gpu': 1, 'mem': 50000*1024**2, 'node': 1}],
        ["billing=12,cpu=6,gres/gpu=1,mem=48G,node=1", { 'billing': 12, 'cpu': 6, 'gpu': 1, 'mem': 48*1024**3, 'node': 1 }],
        ["billing=2.0,cpu=4,gres/gpu=3,mem=50000K,node=2", {'billing': 2.0, 'cpu': 4, 'gpu': 3, 'mem': 50000*1024, 'node': 2}],
        ["billing=1,cpu=2,gres/gpu=1,mem=5T,node=1", {'billing': 1, 'cpu': 2, 'gpu': 1, 'mem': 5*1024**4, 'node': 1}],
        ["billing=1,cpu=2,gres/gpu=1,mem=5P,node=1", {'billing': 1, 'cpu': 2, 'gpu': 1, 'mem': 5*1024**5, 'node': 1}],
    ]
)
def test_parse_sacct_tres(txt: str, expected: dict[str, int]):
    """
    Test parsing of TRES
    """
    tres = Slurm.parse_sacct_tres(txt)
    assert tres == expected

    assert AllocTRES(**tres)
