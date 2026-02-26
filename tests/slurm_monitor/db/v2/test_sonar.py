import pytest
from slurm_monitor.db.v2.sonar import Sonar

@pytest.mark.parametrize("expression, expected_nodes",
    [
        ["simple-node", ["simple-node"]],
        ["simple-node-a,simple-node-b", ["simple-node-a","simple-node-b"]],
        [" simple-node-a , simple-node-b, simple-node-c ", ["simple-node-a","simple-node-b", "simple-node-c"]],
        [" c[1-3,5]-[2-4].fox  ",
         ["c1-2.fox","c1-3.fox","c1-4.fox",
          "c2-2.fox","c2-3.fox","c2-4.fox",
          "c3-2.fox","c3-3.fox","c3-4.fox",
          "c5-2.fox","c5-3.fox","c5-4.fox"
         ]
        ],
        [" simple-node-a , [1,4-5].test",
         ["simple-node-a","1.test", "4.test", "5.test"]
        ]
    ]
)
def test_expand_hostname_range(expression, expected_nodes):
    nodes = Sonar.expand_hostname_range(expression)
    assert set(nodes) == set(expected_nodes)
