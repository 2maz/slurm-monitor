import pytest
from slurm_monitor.db.v2.db_tables import (
    Core,
    Node,
    NodeConfig,
    SoftwareVersion
)
from slurm_monitor.db.v2.db import (
    DatabaseSettings,
    ClusterDB
)
from slurm_monitor.db.v2.db_subscriber import DBJsonImporter
from slurm_monitor.utils.command import Command
import time
from pathlib import Path
import json

@pytest.fixture
def test_data_dir():
    return Path(__file__).parent.parent.parent.parent.resolve() / "data" / "db" / "v2"

@pytest.fixture(scope="module")
def timescaledb(request):
    container_name = "timescaledb-pytest"

    container = Command.run(f"docker ps -f name={container_name} -q")
    if container != "":
        Command.run(f"docker stop {container_name}")

    Command.run(f"docker run -d --rm --name {container_name} -p 7000:5432 -e POSTGRES_DB=test -e POSTGRES_PASSWORD=test -e POSTGRES_USER=test timescale/timescaledb:latest-pg17")

    for i in range(0, 3):
        time.sleep(2)
        container = Command.run(f"docker ps -f name={container_name} -q")
        if container:
            break

    def teardown():
        Command.run(f"docker stop {container_name}")

    request.addfinalizer(teardown)


    return "timescaledb://test:test@localhost:7000/test"


def test_db_setup(timescaledb):
    db_settings = DatabaseSettings(uri=timescaledb)
    db = ClusterDB(db_settings)


    for cluster_id in range(0,5):
        for node_id in range(0,5):
            cluster=f"cluster-{cluster_id}"
            name=f"node-{node_id}"
            node = Node(
                node_id=f"{cluster}.{name}",
                cluster=cluster,
                name=name,
                description=f"{name=} {cluster=}",
                architecture="x86_64"
            )
            db.insert_or_update(node)

            node_config=NodeConfig(
                    node_id=f"{cluster}.{name}",
                    os_name="Linux",
                    os_release="Ubuntu 24.04.1",
                    memory = 128*1000**3,
                    topo_svg = None,
                    cards = [
                        "aaaa-aaaa-aaaa-aaaa"
                    ],
            )
            software_version = SoftwareVersion(
                node_id=f"{cluster}.{name}",
                key='cuda',
                name='cuda library',
                version='12.4'
            )

            core = Core(
                node_id=f"{cluster}.{name}",
                index=0,
                physical=1,
                model="Epic"
            )

            db.insert_or_update(node_config)
            db.insert_or_update(software_version)
            db.insert_or_update(core)

@pytest.mark.parametrize("sysinfo_filename, ps_filename",[
    ["fox-sysinfo.json", "fox-ps.json"],
    ["ml1-sysinfo.json", "ml1-ps.json"]
    ])
def test_db_import_sysinfo(sysinfo_filename, ps_filename, test_data_dir, timescaledb):
    db_settings = DatabaseSettings(uri=timescaledb)
    db = ClusterDB(db_settings)
    importer = DBJsonImporter(db)

    # Ensure that sysinfo is available before ps can be sampled
    sysinfo_json = test_data_dir / sysinfo_filename
    with open(sysinfo_json, 'r') as f:
        data = json.load(f)

    importer.insert(data)

    ps_json = test_data_dir / ps_filename
    with open(ps_json, 'r') as f:
        data = json.load(f)

    importer.insert(data)

#def test_db_visualize(timescaledb):
#
#    #from sqlalchemy_data_model_visualizer import generate_data_model_diagram
#    #models =[
#    #    Core,
#    #    Node,
#    #    NodeConfig,
#    #    GPUCard,
#    #    GPUCardConfig,
#    #    GPUCardProcessStatus,
#    #    ProcessStatus,
#    #    SlurmJobStatus,
#    #    SoftwareVersion
#    #]
#
#    #generate_data_model_diagram(models, "/tmp/test_output_filename.svg", )
#
#    from sqlalchemy import MetaData
#    from sqlalchemy_schemadisplay import create_schema_graph
#
#    # create the pydot graph object by autoloading all tables via a bound metadata object
#    graph = create_schema_graph(metadata=MetaData(timescaledb),
#       show_datatypes=False, # The image would get nasty big if we'd show the datatypes
#       show_indexes=False, # ditto for indexes
#       rankdir='LR', # From left to right (instead of top to bottom)
#       concentrate=False # Don't try to join the relation lines together
#    )
#    graph.write_png('/tmp/dbschema.png') # write out the file
#
