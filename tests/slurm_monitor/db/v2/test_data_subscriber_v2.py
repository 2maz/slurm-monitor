import pytest
import platform

from slurm_monitor.db.v2.db_tables import TableBase
from slurm_monitor.db.v2.data_subscriber import expand_node_names
from slurm_monitor.utils import utcnow
from slurm_monitor.db.v2.data_subscriber import DBJsonImporter
from slurm_monitor.utils.system_info import SystemInfo
import psutil
import copy
import sqlalchemy
import json
from pathlib import Path

@pytest.mark.parametrize("names,nodes",[
    ["n[001-003,056],gh001",["n001","n002","n003","n056","gh001"]]
])
def test_expand_node_names(names, nodes):
    assert sorted(expand_node_names(names)) == sorted(nodes)


def sonar_base_message(message_type: str, **kwargs):
    return {
        'meta': { 'producer': 'sonar', 'version': '0.16.0' },
        'data': { 'type': message_type, 'attributes': kwargs },
        'errors': {}
    }

def sonar_sample_message():
    return sonar_base_message('sample',
        node='c-0-n-0',
        cluster='c-0',
        system={},
        jobs=[
          { "job": 1,
            "user": "test-user",
            "epoch": 1,
            "processes": [{
              'resident_memory': 2*1024**3,
              'virtual_memory': 32*1024**3,
              'cmd': "test-cmd",
              'pid': 10000,
              'ppid': 9999,
              'num_threads': 1,
              'cpu_avg': 20,
              'cpu_util': 20,
              'cpu_time': 1000,
              'data_read': 0,
              'data_written': 0,
              'data_cancelled': 0,
              'rolledup': 0,
              }]
            },
            { "job": 2,
              "user": "test-user",
              "epoch": 2,
              "processes": [{
                'resident_memory': 2*1024**3,
                'virtual_memory': 32*1024**3,
                'cmd': "test-cmd",
                'pid': 10000,
                'ppid': 9999,
                'num_threads': 1,
                'cpu_avg': 20,
                'cpu_util': 20,
                'cpu_time': 1000,
                'data_read': 0,
                'data_written': 0,
                'data_cancelled': 0,
                'rolledup': 0,
                }]
             }
        ],
        time=str(utcnow())
    )

def sonar_sysinfo_message():
    si = SystemInfo()
    return sonar_base_message('sysinfo',
                       cluster='c-0',
                       node='c-0-n-0',
                       os_name=platform.uname().system,
                       os_release=platform.uname().version,
                       architecture=platform.machine(),
                       sockets=2,
                       cores_per_socket=2,
                       threads_per_core=4,
                       cpu_model=si.cpu_info.model,
                       memory=psutil.virtual_memory().total / 1024, # now in KiB
                       cards=[
                           {
                               'uuid': 'c-0-n-0-g-0',
                               'manufacturer': 'Nvidia',
                               'model': 'A100-80GB',
                               'architecture': 'AMPERE',
                               'memory': 80*1024**2,
                               'index': 0,
                               'address': 'pci:0',
                               'driver': 'testdriver',
                               'firmware': '1.0.1',
                               'power_limit': 250,
                               'max_power_limit': 350,
                               'min_power_limit': 100,
                               'max_ce_clock': 2500,
                               'max_memory_clock': 2500,
                           }
                       ],
                       distances=[],
                       time=str(utcnow()),
    )

@pytest.mark.parametrize("sonar_msg, expected_samples",[
    [sonar_sysinfo_message(), 4],

])
def test_DBJsonImporter_extra_attributes(sonar_msg, expected_samples, test_db_v2):
    importer = DBJsonImporter(db=test_db_v2)
    msg = importer.to_message(sonar_msg)

    TableBase.__extra_values__ = 'forbid'
    samples = importer.parse_sysinfo(copy.deepcopy(msg))
    assert len(samples) == expected_samples

    TableBase.__extra_values__ = 'allow'
    msg.data.attributes["extra_attribute"] = "unknown"
    samples = importer.parse_sysinfo(copy.deepcopy(msg))
    assert len(samples) == expected_samples

    TableBase.__extra_values__ = 'forbid'
    with pytest.raises(TypeError):
        importer.parse_sysinfo(copy.deepcopy(msg))


@pytest.mark.asyncio(loop_scope="module")
@pytest.mark.parametrize("sonar_msg",
    [sonar_sample_message()]
)
async def test_DBJsonImporter_non_slurm(sonar_msg, test_db_v2):
    nodes = []
    for node in ["c-0-n-1", "c-0-n-2"]:
        sonar_msg['data']['attributes']['node'] = node
        cluster = sonar_msg['data']['attributes']['cluster']
        importers = []
        for i in range(0, 3):
            importers.append(DBJsonImporter(db=test_db_v2))

        with test_db_v2.make_session() as session:
            results = session.execute(sqlalchemy.text(f"SELECT * from cluster_attributes WHERE cluster='{cluster}'")).all()
            assert len(results) == len(nodes), f"Expected {len(nodes)} cluster_attributes entries got {[x.cluster for x in results]}"

        nodes.append(node)
        for i in range(0,10):
            for importer in importers:
                await importer.insert(copy.deepcopy(sonar_msg))

            with test_db_v2.make_session() as session:
                results = session.execute(sqlalchemy.text(f"SELECT * from cluster_attributes WHERE cluster='{cluster}'")).all()
                assert len(results) == len(nodes), f"Update #{i}: expected {len(nodes)} cluster_attributes entries got {[x.cluster for x in results]}"

@pytest.mark.asyncio(loop_scope="module")
@pytest.mark.parametrize("sonar_msg_files",
    [
        [
         "0+sysinfo-ml1.hpc.uio.no.json",
         "0+sample-ml1.hpc.uio.no.json"
        ],
    ]
)
async def test_DBJsonImporter_sonar_examples(sonar_msg_files, test_db_v2, test_data_dir):
    importer = DBJsonImporter(db=test_db_v2)

    in_msg_uuids = []
    for sonar_msg_file in sonar_msg_files:
        json_filename = Path(test_data_dir) / "sonar" / sonar_msg_file
        with open(json_filename, "r") as f:
            msg_data = json.load(f)
            if 'cards' in msg_data['data']['attributes']:
                in_msg_uuids = [x['uuid'] for x in msg_data['data']['attributes']['cards']]

            await importer.insert(copy.deepcopy(msg_data))

    with test_db_v2.make_session() as session:
        results = session.execute(sqlalchemy.text("SELECT * from cluster_attributes WHERE 'ml1.hpc.uio.no' = ANY(nodes)")).all()
        assert len(results) == 1, f"Expected 1 matching cluster_attributes entries got {[x.cluster for x in results]}"

        results = session.execute(sqlalchemy.text("SELECT * from node WHERE node = 'ml1.hpc.uio.no'")).all()
        assert len(results) == 1, f"Expected 1 matching node entry go {results}"

        results = session.execute(sqlalchemy.text("SELECT uuid from sysinfo_gpu_card")).all()
        in_db_uuids = [x[0] for x in results]

    for uuid in in_msg_uuids:
        assert uuid in in_db_uuids, f"Expected uuids {in_msg_uuids} to be present, but found {in_db_uuids}"
