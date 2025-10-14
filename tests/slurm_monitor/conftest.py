import pytest
import json
import subprocess
import psutil
import datetime as dt
from pathlib import Path
import sqlalchemy
import time

import slurm_monitor.timescaledb # noqa
from slurm_monitor.utils import utcnow
from slurm_monitor.devices.gpu import GPU
from slurm_monitor.utils.command import Command

from slurm_monitor.db.v1.db import SlurmMonitorDB, DatabaseSettings
from slurm_monitor.db.v1.db_tables import (
        CPUStatus,
        GPUs,
        GPUStatus,
        JobStatus,
        LocalIndexedGPUs,
        MemoryStatus,
        Nodes,
        ProcessStatus
)

import slurm_monitor.db.v2 as db_v2
import slurm_monitor.db.v2.db_testing as db_v2_testing

def pytest_addoption(parser):
    parser.addoption("--db-uri", help="Test database ", default=None)

@pytest.fixture(scope='module')
def monkeypatch_module():
    with pytest.MonkeyPatch.context() as mp:
        yield mp

@pytest.fixture(scope="session")
def mock_slurm_command_hint():
    return Path(__file__).parent.parent / "mock"

@pytest.fixture(scope='module')
def number_of_nodes() -> int:
    return 5

@pytest.fixture(scope='module')
def number_of_gpus() -> int:
    return 2

@pytest.fixture(scope='module')
def number_of_cpus() -> int:
    return 2

@pytest.fixture(scope='module')
def number_of_samples() -> int:
    return 50

@pytest.fixture(scope="module")
def test_db_uri(tmp_path_factory, pytestconfig):
    db_uri = pytestconfig.getoption("db_uri")
    if not db_uri:
        path = tmp_path_factory.mktemp("data") / "slurm-monitor.test.sqlite"
        return f"sqlite:///{path}"
    return db_uri

@pytest.fixture(scope="module")
def test_db(test_db_uri, number_of_nodes, number_of_cpus, number_of_gpus, number_of_samples) -> SlurmMonitorDB:
    db_settings = DatabaseSettings(uri=test_db_uri)
    db = SlurmMonitorDB(db_settings)

    db.clear()

    virtual_memory = psutil.virtual_memory()._asdict()
    for i in range(0, number_of_nodes):
        nodename = f"node-{i}"
        db.insert_or_update(Nodes(name=nodename, cpu_count=number_of_cpus, cpu_model='Intel Xeon', memory_total=256*1024**2))

        start_time = utcnow() - dt.timedelta(seconds=number_of_samples)
        for c in range(0, number_of_cpus):
            for s in range(0, number_of_samples):
                sample = CPUStatus(
                    node=nodename,
                    local_id=c,
                    cpu_percent=25,
                    timestamp=start_time + dt.timedelta(seconds=s)
                )
                db.insert(sample)

        for s in range(0, number_of_samples):
            sample = MemoryStatus(
                **virtual_memory,
                node=nodename,
                timestamp=start_time + dt.timedelta(seconds=s)
            )
            db.insert(sample)

        for g in range(0, number_of_gpus):
            for s in range(0, number_of_samples):
                uuid=f"GPU-{nodename}:{g}"
                db.insert_or_update(GPUs(
                    uuid=uuid,
                    model="Tesla V100",
                    local_id=g,
                    node=nodename,
                    memory_total=16*1024**3
                ))
                db.insert_or_update(LocalIndexedGPUs(
                    uuid=uuid,
                    local_id=g,
                    node=nodename,
                    start_time=dt.datetime(2024,6,1),
                    end_time=dt.datetime(2050,5,31),
                ))

                sample = GPUStatus(
                        uuid=uuid,
                        power_draw=30,
                        temperature_gpu=30,
                        utilization_memory=10,
                        utilization_gpu=12,
                        timestamp=start_time + dt.timedelta(seconds=s)
                )
                db.insert(sample)

        job_id = i
        sample_count = 100

        end_time = utcnow()
        start_time = end_time - dt.timedelta(seconds=sample_count)
        submit_time = start_time - dt.timedelta(seconds=20)
        db.insert(JobStatus(
            job_id=job_id,
            submit_time=submit_time,
            name="test-job",
            start_time=start_time,
            end_time=end_time,
            account="account",
            accrue_time=10000,
            admin_comment="",
            array_job_id=1,
            array_task_id=1,
            array_max_tasks=20,
            array_task_string="",
            association_id=1,
            batch_host=nodename,
            cluster="slurm",
            derived_exit_code=0,
            eligible_time=1000,
            exit_code=0,
            gres_detail=[],
            group_id=0,
            job_state="COMPLETED",
            nodes=nodename,
            cpus=1,
            node_count=1,
            tasks=1,
            partition="slowq",
            state_description="",
            state_reason="",
            suspend_time=0,
            time_limit=7200,
            user_id=1000,
            )
        )

        for pid in range(1, 10):
            samples = []
            for idx in range(1, sample_count+1):
                timestamp = end_time - dt.timedelta(seconds=idx)
                samples.append(ProcessStatus(
                        pid=pid, node=nodename,
                        job_id=job_id, job_submit_time=submit_time,
                        cpu_percent=0.5, memory_percent=0.2,
                        timestamp=timestamp
                        )
                )
            samples.reverse()
            db.insert(samples)
    return db




ROCM_SMI_RESPONSE_V1=[
    "device,Unique ID,"
    "Temperature (Sensor edge) (C),Temperature (Sensor junction) (C),Temperature (Sensor memory) (C),"
    "Temperature (Sensor HBM 0) (C),Temperature (Sensor HBM 1) (C),Temperature (Sensor HBM 2) (C),"
    "Temperature (Sensor HBM 3) (C),"
    "Average Graphics Package Power (W),GPU use (%),GFX Activity,"
    "GPU memory use (%),Memory Activity,Voltage (mV),VRAM Total Memory (B),VRAM Total Used Memory (B),"
    "Card series,Card model,Card vendor,Card SKU",
    "card0,0x2490210172fc1a88,33.0,34.0,32.0,0.0,0.0,0.0,0.0,19.0,0,0,0,0,762,17163091968,10854400,"
    "Vega 20 [Radeon Pro VII/Radeon Instinct MI50 32GB],0x081e,Advanced Micro Devices Inc. [AMD/ATI],D1640600"
]

ROCM_SMI_RESPONSE_V2=[
    "WARNING: Unlocked monitor_devices lock; it should have already been unlocked.",
    "device,Unique ID,Temperature (Sensor edge) (C),Temperature (Sensor junction) (C),Temperature (Sensor memory) (C),"
    "Temperature (Sensor HBM 0) (C),Temperature (Sensor HBM 1) (C),Temperature (Sensor HBM 2) (C),"
    "Temperature (Sensor HBM 3) (C),"
    "Average Graphics Package Power (W),GPU use (%),GFX Activity,GPU memory use (%),"
    "Memory Activity,Voltage (mV),VRAM Total Memory (B),VRAM Total Used Memory (B),"
    "Card series,Card model,Card vendor,Card SKU",
    "card0,0x18f68e602b8a790f,41.0,44.0,53.0,52.0,50.0,53.0,52.0,41.0,0,391,0,0,793,68702699520,10960896,"
    "Instinct MI210,0x0c34,Advanced Micro Devices Inc. [AMD/ATI],D67301",
    "card1,0x95a1ca7691e7c391,44.0,44.0,49.0,49.0,48.0,49.0,49.0,39.0,0,415,0,0,793,68702699520,10960896,"
    "Instinct MI210,0x0c34,Advanced Micro Devices Inc. [AMD/ATI],D67301",
]

HL_SMI_RESPONSE=[
    "name, uuid, power.draw, temperature.aip, utilization.aip, memory.used, memory.total",
    "HL-205, 00P1-HL2000A1-14-P64W99-13-05-03, 103, 36, 15, 512, 32768",
    "HL-205, 00P1-HL2000A1-14-P64W99-02-08-05, 105, 34, 15, 512, 32768",
    "HL-205, 00P1-HL2000A1-14-P64X78-02-07-10, 96, 33, 12, 512, 32768",
    "HL-205, 00P1-HL2000A1-14-P64X00-12-04-07, 100, 32, 13, 512, 32768",
    "HL-205, 00P1-HL2000A1-14-P64W98-10-03-11, 103, 34, 15, 512, 32768",
    "HL-205, 00P1-HL2000A1-14-P64X00-16-07-11, 102, 30, 14, 512, 32768",
    "HL-205, 00P1-HL2000A1-14-P64W98-05-02-04, 103, 31, 14, 512, 32768",
    "HL-205, 00P1-HL2000A1-14-P64X00-08-05-09, 103, 38, 14, 512, 32768",
]


GPU_RESPONSES = {
    "nvidia":
        {
            'discover': [
                    "Tesla V100-SXM3-32GB, 32768",
                    "Tesla V100-SXM3-32GB, 32768"
             ],
            'probe': [
                "name, uuid, power.draw [W], temperature.gpu, utilization.gpu [%], utilization.memory [%], memory.used [MiB], memory.free [MiB]",
                "Tesla V100-SXM3-32GB, GPU-ad466f2f-575d-d949-35e0-9a7d912d974e, 48.99, 30, 0, 0, 5, 32495",
                "Tesla V100-SXM3-32GB, GPU-3410aa41-502b-5cf1-d8c4-58dc1c22fbc9, 383.55, 62, 98, 35, 11149, 21351",
                "Tesla V100-SXM3-32GB, GPU-eb3c30c2-a906-1551-1c60-f0a3e3221001, 56.10, 51, 0, 0, 7, 32493",
                "Tesla V100-SXM3-32GB, GPU-6c4a2c5f-b2d4-33eb-adbc-c282f0e1c2ad, 49.69, 35, 0, 0, 5, 32495",
                "Tesla V100-SXM3-32GB, GPU-884e24ec-717d-5596-0a75-cdb279251a65, 191.77, 51, 64, 18, 2513, 29987",
                "Tesla V100-SXM3-32GB, GPU-f184e8d3-dbd3-224f-a6e0-a66252975282, 54.00, 46, 0, 0, 5, 32495",
                "Tesla V100-SXM3-32GB, GPU-acd22660-c053-cb79-747a-be2026681d9b, 50.72, 29, 0, 0, 5, 32495",
                "Tesla V100-SXM3-32GB, GPU-0a4d23ff-bbc5-b048-bcd5-db96716c86a2, 352.89, 66, 100, 48, 19463, 13037",
                "Tesla V100-SXM3-32GB, GPU-472ba829-d37c-a6c8-a2b8-db1bfee232bd, 66.92, 33, 13, 0, 445, 32055",
                "Tesla V100-SXM3-32GB, GPU-6522bf6c-9834-766d-8e13-da8416ba6a77, 48.45, 31, 0, 0, 5, 32495",
                "Tesla V100-SXM3-32GB, GPU-b12c6a1a-e055-02e8-84a3-107cf392b7d8, 52.17, 37, 0, 0, 5, 32495",
                "Tesla V100-SXM3-32GB, GPU-97062550-625d-1c91-43ed-05d5efb2e4b5, 49.76, 36, 0, 0, 5, 32495",
                "Tesla V100-SXM3-32GB, GPU-3dc3397d-6caa-8a32-1299-aa3166d0d578, 238.71, 54, 58, 32, 6439, 26061",
                "Tesla V100-SXM3-32GB, GPU-a69a93b4-8e1e-db24-5cd8-5d94f6374df9, 49.12, 31, 0, 0, 5, 32495",
                "Tesla V100-SXM3-32GB, GPU-83b0c1d3-23c8-b85e-c5f7-19bbef62931c, 52.23, 46, 0, 0, 5, 32495",
                "Tesla V100-SXM3-32GB, GPU-25ed1520-6fbc-ae62-814d-64ea9098b047, 51.17, 37, 0, 0, 5, 32495",
            ],
            'process-status': [
                "gpu_uuid, pid, process_name, used_gpu_memory [MiB]",
                "GPU-ad466f2f-575d-d949-35e0-9a7d912d974e, 1, /home/auser/D1/miniconda3/envs/vLLM_test_env/bin/python, 8032",
            ],
            'expect': {
                'gpu_count': 16,
                'gpu_model': 'Tesla V100-SXM3-32GB',
                'gpu_memory': 32768,
                'gpu_processes': 1,
            }
        },
    "rocm":
        {
            'discover': [
                "device,VRAM Total Memory (B),VRAM Total Used Memory (B),Card series,Card model,Card vendor,Card SKU"
                "card0,68702699520,10960896,Instinct MI210,0x0c34,Advanced Micro Devices Inc. [AMD/ATI],D67301",
                "card1,68702699520,10960896,Instinct MI210,0x0c34,Advanced Micro Devices Inc. [AMD/ATI],D67301",
                ],
            'probe': [
                "device,Unique ID,Temperature (Sensor edge) (C),Temperature (Sensor junction) (C),Temperature (Sensor memory) (C),Temperature (Sensor HBM 0) (C),Temperature (Sensor HBM 1) (C),Temperature (Sensor HBM 2) (C),Temperature (Sensor HBM 3) (C),Average Graphics Package Power (W),GPU use (%),GFX Activity,GPU memory use (%),Memory Activity,Voltage (mV),VRAM Total Memory (B),VRAM Total Used Memory (B),Card series,Card model,Card vendor,Card SKU",
                "card0,0x67c63ac3c3c637f9,45.0,45.0,50.0,50.0,50.0,49.0,47.0,36.0,0,858010810,0,115487143,718,68702699520,10960896,Instinct MI210,0x0c34,Advanced Micro Devices Inc. [AMD/ATI],D67301",
                "card1,0x7b6b270993774982,33.0,40.0,52.0,51.0,50.0,50.0,51.0,40.0,0,781368565,0,147407533,793,68702699520,10960896,Instinct MI210,0x0c34,Advanced Micro Devices Inc. [AMD/ATI],D67301",
            ],
            'showpidgpus': [
                "=============================== GPUs Indexed by PID ================================",
                "PID 3903703 is using 2 DRM device(s):",
                "0 1",
                "====================================================================================",
                "=============================== End of ROCm SMI Log ================================"
            ],
            'showpids': [
                "name, value",
                '"PID3903703", "python, 2, 12333932544, 0, 11"'
            ],
            'expect': {
                'gpu_count': 2,
                'gpu_model': "Instinct MI210",
                'gpu_memory': 68702699520/1024.0**2,
                'gpu_processes': 2,
            }
        },
    "hl":
        {
            'plain' : [
                "+-----------------------------------------------------------------------------+",
                "| HL-SMI Version:                              hl-1.18.0-fw-53.1.1.1          |",
                "| Driver Version:                                     1.18.0-ee698fb          |",
                "|-------------------------------+----------------------+----------------------+",
                "| AIP  Name        Persistence-M| Bus-Id        Disp.A | Volatile Uncorr. ECC |",
                "| Fan  Temp  Perf  Pwr:Usage/Cap|         Memory-Usage | AIP-Util  Compute M. |",
                "|===============================+======================+======================|",
                "|   0  HL-205              N/A  | 0000:b3:00.0     N/A |                   0  |",
                "| N/A   33C   N/A   104W / 350W |    512MiB / 32768MiB |    15%           N/A |",
                "|-------------------------------+----------------------+----------------------+",
                "|   1  HL-205              N/A  | 0000:b4:00.0     N/A |                   0  |",
                "| N/A   30C   N/A   102W / 350W |    512MiB / 32768MiB |    14%           N/A |",
                "|-------------------------------+----------------------+----------------------+",
                "|   2  HL-205              N/A  | 0000:cd:00.0     N/A |                   0  |",
                "| N/A   37C   N/A   104W / 350W |    512MiB / 32768MiB |    15%           N/A |",
                "|-------------------------------+----------------------+----------------------+",
                "|   3  HL-205              N/A  | 0000:cc:00.0     N/A |                   0  |",
                "| N/A   30C   N/A   103W / 350W |    512MiB / 32768MiB |    15%           N/A |",
                "|-------------------------------+----------------------+----------------------+",
                "|   4  HL-205              N/A  | 0000:19:00.0     N/A |                   0  |",
                "| N/A   33C   N/A    96W / 350W |    512MiB / 32768MiB |    12%           N/A |",
                "|-------------------------------+----------------------+----------------------+",
                "|   5  HL-205              N/A  | 0000:1a:00.0     N/A |                   0  |",
                "| N/A   33C   N/A   105W / 350W |    512MiB / 32768MiB |    15%           N/A |",
                "|-------------------------------+----------------------+----------------------+",
                "|   6  HL-205              N/A  | 0000:34:00.0     N/A |                   0  |",
                "| N/A   36C   N/A   103W / 350W |    512MiB / 32768MiB |    15%           N/A |",
                "|-------------------------------+----------------------+----------------------+",
                "|   7  HL-205              N/A  | 0000:33:00.0     N/A |                   0  |",
                "| N/A   32C   N/A   100W / 350W |    512MiB / 32768MiB |    13%           N/A |",
                "|-------------------------------+----------------------+----------------------+",
                "| Compute Processes:                                               AIP Memory |",
                "|  AIP       PID   Type   Process name                             Usage      |",
                "|=============================================================================|",
                "|   0        10    C      python name                              1024MiB    |",
                "|   1        N/A   N/A    N/A                                      N/A        |",
                "|   2        N/A   N/A    N/A                                      N/A        |",
                "|   3        N/A   N/A    N/A                                      N/A        |",
                "|   4        N/A   N/A    N/A                                      N/A        |",
                "|   5        N/A   N/A    N/A                                      N/A        |",
                "|   6        20    c      custom process                           2040MiB    |",
                "|   7        N/A   N/A    N/A                                      N/A        |",
                "+=============================================================================+",
            ],
            'discover' : [
                "HL-205, 32768",
                "HL-205, 32768",
                "HL-205, 32768",
                "HL-205, 32768",
                "HL-205, 32768",
                "HL-205, 32768",
                "HL-205, 32768",
                "HL-205, 32768",
            ],
            'probe' : [
                "name, uuid, power.draw [W], temperature.aip [C], utilization.aip [%], memory.used [MiB], memory.total [MiB]",
                "HL-205, 00P1-HL2000A1-14-P64X00-16-07-11, 102, 29, 14, 512, 32768",
                "HL-205, 00P1-HL2000A1-14-P64W98-10-03-11, 103, 32, 15, 512, 32768",
                "HL-205, 00P1-HL2000A1-14-P64W98-05-02-04, 103, 30, 14, 512, 32768",
                "HL-205, 00P1-HL2000A1-14-P64X00-08-05-09, 103, 35, 15, 512, 32768",
                "HL-205, 00P1-HL2000A1-14-P64X78-02-07-10, 96, 31, 12, 512, 32768",
                "HL-205, 00P1-HL2000A1-14-P64W99-02-08-05, 105, 33, 15, 512, 32768",
                "HL-205, 00P1-HL2000A1-14-P64X00-12-04-07, 100, 31, 13, 512, 32768",
                "HL-205, 00P1-HL2000A1-14-P64W99-13-05-03, 103, 34, 14, 512, 32768",
            ],
            'expect': {
                'gpu_count': 8,
                'gpu_model': "HL-205",
                'gpu_memory': 32768,
                'gpu_processes': 2,
            }
        },
    "xpu":
        {
            'discover':
            [
                {
                    "device_list": [
                        {
                            "device_function_type": "physical",
                            "device_id": 0,
                            "device_name": "Intel(R) Data Center GPU Max 1100",
                            "device_type": "GPU",
                            "drm_device": "/dev/dri/card1",
                            "pci_bdf_address": "0000:29:00.0",
                            "pci_device_id": "0xbda",
                            "uuid": "00000000-0000-0029-0000-002f0bda8086",
                            "vendor_name": "Intel(R) Corporation"
                        },
                        {
                            "device_function_type": "physical",
                            "device_id": 1,
                            "device_name": "Intel(R) Data Center GPU Max 1100",
                            "device_type": "GPU",
                            "drm_device": "/dev/dri/card2",
                            "pci_bdf_address": "0000:9a:00.0",
                            "pci_device_id": "0xbda",
                            "uuid": "00000000-0000-009a-0000-002f0bda8086",
                            "vendor_name": "Intel(R) Corporation"
                        }
                    ]
                },
                {
                    "device_id": 0,
                    "device_name": "Intel(R) Data Center GPU Max 1100",
                    "device_stepping": "B4",
                    "device_type": "GPU",
                    "driver_version": "I915_23.10.72_PSB_231129.76",
                    "drm_device": "/dev/dri/card1",
                    "gfx_data_firmware_name": "GFX_DATA",
                    "gfx_data_firmware_version": "",
                    "gfx_firmware_name": "GFX",
                    "gfx_firmware_status": "normal",
                    "gfx_firmware_version": "unknown",
                    "gfx_pscbin_firmware_name": "GFX_PSCBIN",
                    "gfx_pscbin_firmware_version": "",
                    "kernel_version": "5.15.0-122-generic",
                    "max_command_queue_priority": "0",
                    "max_hardware_contexts": "65536",
                    "max_mem_alloc_size_byte": "48946688000",
                    "memory_bus_width": "128",
                    "memory_ecc_state": "enabled",
                    "memory_free_size_byte": "51493543936",
                    "memory_physical_size_byte": "51522830336",
                    "number_of_slices": "1",
                    "number_of_sub_slices_per_slice": "56",
                    "number_of_tiles": "1",
                    "serial_number": "unknown",
                    "sku_type": "",
                    "uuid": "00000000-0000-0029-0000-002f0bda8086",
                    "vendor_name": "Intel(R) Corporation",
                }
            ],
            'probe': [
                    "Timestamp, DeviceId, GPU Utilization (%), GPU Power (W), GPU Frequency (MHz), GPU Core Temperature (Celsius Degree), GPU Memory Temperature (Celsius Degree), GPU Memory Utilization (%), GPU Memory Used (MiB)",
                    "14:32:57.448,    0, 0.00, 106.60,    0,  N/A,  N/A, 0.05, 28.02",
                    "14:32:57.449,    1, 0.01, 105.25,    0,  N/A,  N/A, 0.05, 28.02",
            ],
            'expect': {
                    'gpu_count': 2,
                    'gpu_memory': int(51522830336 / 1024.0**2),
                    'gpu_model': "Intel(R) Data Center GPU Max 1100",
                    'gpu_processes': 0,
            }
        }
    }

@pytest.fixture
def gpu_responses():
    return GPU_RESPONSES

@pytest.fixture
def mock_gpu(gpu_type, gpu_responses, monkeypatch):
    # Disable existing python libraries that could be use for retrieval
    try:
        def mock_force_fail():
            raise RuntimeError("Test: forced error")

        import pynvml
        monkeypatch.setattr(pynvml, "nvmlInit", mock_force_fail)
    except (ImportError, NameError):
        pass

    # Disable existing python libraries that could be use for retrieval
    try:
        def mock_force_fail():
            raise RuntimeError("Test: forced error")

        from pyrsmi import rocml
        monkeypatch.setattr(rocml, "smi_initialize", mock_force_fail)
    except (ImportError, NameError):
        pass

    orig_subprocess_run = subprocess.run
    def mock_subprocess_run(cmd, **kwargs):
        if type(cmd) == str:
            if cmd.startswith(f"command -v {gpu_type}"):
                return orig_subprocess_run("echo 'cmd'", **kwargs)
            elif cmd.startswith("command -v"):
                return orig_subprocess_run("command -v foobar", **kwargs)

            smi_response = gpu_responses[gpu_type]['discover']
            # XPU Specifics - since memory total can only be identified using 'discovery'
            if gpu_type == "xpu":
                if cmd == "xpu-smi discovery -j":
                    smi_response = json.dumps(smi_response[0])
                elif cmd.startswith("xpu-smi discovery -d"):
                    smi_response = json.dumps(smi_response[1])
            else:
                smi_response = '\n'.join(smi_response)

            if cmd.startswith(f"{gpu_type}-smi"):
                if cmd == f"{gpu_type}-smi":
                    if 'plain' in gpu_responses[gpu_type]:
                        smi_response = '\n'.join(gpu_responses[gpu_type]['plain'])
                elif "--query-compute-apps" in cmd: # nvidia
                    smi_response = '\n'.join(gpu_responses[gpu_type]['process-status'])
                elif "--showpids" in cmd: # rocm
                    smi_response = '\n'.join(gpu_responses[gpu_type]['showpids'])
                elif "--showpidgpus" in cmd:
                    smi_response = '\n'.join(gpu_responses[gpu_type]['showpidgpus'])

                return orig_subprocess_run(f"echo '{smi_response}'", **kwargs)

        return orig_subprocess_run(cmd, **kwargs)

    monkeypatch.setattr(subprocess, "run", mock_subprocess_run)

    def mock_query_status_smi(self) -> str:
        smi_response = '\n'.join(gpu_responses[gpu_type]['probe'])
        return Command.run(f"echo '{smi_response}'")

    monkeypatch.setattr(GPU, "query_status_smi", mock_query_status_smi)

    return gpu_responses[gpu_type]['expect']

## DB V2
@pytest.fixture(scope='module')
def db_config() -> db_v2_testing.TestDBConfig:
    return db_v2_testing.TestDBConfig()

@pytest.fixture(scope="module")
def timescaledb(request):
    container_name = "timescaledb-pytest"
    port=7001
    # using this mechanism one can parametrize the fixture via
    #   @pytest.mark.parametrize("timescaledb", [{'port': 7002}], indirect=["timescaledb"])
    #   def test_XXX(timescaledb, ...):
    #
    if hasattr(request, "param"):
        if 'port' in request.param:
            port = request.param['port']
        if 'container-suffix' in request.param:
            container_name += request.param['container-suffix']

    uri = db_v2_testing.start_timescaledb_container(
            port=port,
            container_name=container_name
    )

    def teardown():
        Command.run(f"docker stop {container_name}")

    request.addfinalizer(teardown)
    return uri

@pytest.fixture(scope="module")
def test_db_v2(timescaledb,
        db_config) -> db_v2.db.ClusterDB:
    for i in range(0,3):
        try:
            db_test = db_v2_testing.create_test_db(timescaledb, db_config)
            time.sleep(2)
        except sqlalchemy.exc.OperationalError as e:
            if i < 2 and "server closed" in str(e):
                pass
            raise
    return db_test
