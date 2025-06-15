import pytest
from slurm_monitor.db.v2.data_subscriber import expand_node_names

@pytest.mark.parametrize("names,nodes",[
    ["n[001-003,056],gh001",["n001","n002","n003","n056","gh001"]]
])
def test_expand_node_names(names, nodes):
    assert sorted(expand_node_names(names)) == sorted(nodes)
