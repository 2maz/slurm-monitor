import pytest
import platform

from slurm_monitor.db.v2.db_tables import TableBase
from slurm_monitor.db.v2.data_subscriber import expand_node_names
from slurm_monitor.utils import utcnow
from slurm_monitor.db.v2.data_subscriber import DBJsonImporter
from slurm_monitor.utils.system_info import SystemInfo
import psutil
import copy

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
def test_DBJsonImporter(sonar_msg, expected_samples, test_db_v2):
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
