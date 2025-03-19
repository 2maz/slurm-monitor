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

