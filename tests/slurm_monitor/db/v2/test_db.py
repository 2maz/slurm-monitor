import pytest
from slurm_monitor.db.v2.db_tables import (
    GPUCard,
    GPUCardConfig,
    GPUCardProcessStatus,
    Node,
    NodeConfig,
    ProcessStatus,
    SlurmJobStatus
)
from slurm_monitor.db.v2.db import (
    DatabaseSettings,
    ClusterDB
)

def test_db_setup(test_db_uri):
    db_settings = DatabaseSettings(uri=test_db_uri)
    db = ClusterDB(db_settings)

    for cluster_id in range(0,5):
        for node_id in range(0,5):
            cluster=f"cluster-{cluster_id}"
            name=f"node-{node_id}"
            node = Node(
                cluster=cluster,
                name=name,
                description=f"{name=} {cluster=}",
                architecture="x86_64"
            )
            db.insert_or_update(node)

            node_config=NodeConfig(
                    cluster=cluster,
                    node=name,
                    os_name="Linux",
                    os_release="Ubuntu 24.04.1",
                    cores = [
                        {
                            'index': 0,
                            'physical': 1,
                            'model': 'Epic'
                        }
                    ],
                    memory = 128*1000**3,
                    topo_svg = None,
                    cards = [
                        "aaaa-aaaa-aaaa-aaaa"
                    ],
                    software = [
                        {
                            'key':'cuda',
                            'name':'cuda library',
                            'version':'12.4'
                        },
                        {
                            'key':'cuda',
                            'name':'cuda library',
                            'version':'12.2'
                        },
                    ],
                    #timestamp
            )
            db.insert_or_update(node_config)



def test_db_visualize():

    from sqlalchemy_data_model_visualizer import generate_data_model_diagram
    models =[
        Node,
        NodeConfig,
        GPUCard,
        GPUCardConfig,
        GPUCardProcessStatus,
        ProcessStatus,
        SlurmJobStatus
    ]

    generate_data_model_diagram(models, "/tmp/test_output_filename.svg")

