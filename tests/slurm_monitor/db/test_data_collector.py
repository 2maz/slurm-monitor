import pytest
import time

from slurm_monitor.db.v0.data_collector import (
        Observer, Observable, GPUStatusCollector, GPUStatusCollectorPool,
        NvidiaInfoCollector, ROCMInfoCollector, HabanaInfoCollector
)
from slurm_monitor.db.v0.db_tables import GPUStatus
from slurm_monitor.db.v0.db import DatabaseSettings, SlurmMonitorDB
from pathlib import Path
from slurm_monitor.utils import utcnow

@pytest.fixture
def db_path(tmp_path):
    return Path(tmp_path) / "slurm-monitor-db.sqlite"

@pytest.fixture
def db_settings(db_path):
    return DatabaseSettings(uri=f"sqlite:///{db_path}")

def test_Observer_Observable():
    observer = Observer[int]()
    observable = Observable[int](observers=[observer])

    for i in range(0,100):
        observable.notify_all([i])
        assert observer._samples.get() == i

NVIDIA_SMI_RESPONSE=[
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
]

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
    "HL-205, 00P1-HL2000A1-14-P64W99-13-05-03, 103, 36, 15, 512, 32768",
    "HL-205, 00P1-HL2000A1-14-P64W99-02-08-05, 105, 34, 15, 512, 32768",
    "HL-205, 00P1-HL2000A1-14-P64X78-02-07-10, 96, 33, 12, 512, 32768",
    "HL-205, 00P1-HL2000A1-14-P64X00-12-04-07, 100, 32, 13, 512, 32768",
    "HL-205, 00P1-HL2000A1-14-P64W98-10-03-11, 103, 34, 15, 512, 32768",
    "HL-205, 00P1-HL2000A1-14-P64X00-16-07-11, 102, 30, 14, 512, 32768",
    "HL-205, 00P1-HL2000A1-14-P64W98-05-02-04, 103, 31, 14, 512, 32768",
    "HL-205, 00P1-HL2000A1-14-P64X00-08-05-09, 103, 38, 14, 512, 32768",
]

@pytest.mark.parametrize(
        "cls,response,gpu_count",
        [
            [NvidiaInfoCollector, NVIDIA_SMI_RESPONSE, 16],
            [ROCMInfoCollector, ROCM_SMI_RESPONSE_V1, 1],
            [ROCMInfoCollector, ROCM_SMI_RESPONSE_V2, 2],
            [HabanaInfoCollector, HL_SMI_RESPONSE, 8],
        ]
)
def test_GPUStatusCollector(cls: GPUStatusCollector, response: list[str], gpu_count: int, monkeypatch):
    observer = Observer[GPUStatus]()
    response = '\n'.join(response)

    def mock_send_request(self, nodename: str, user: str) -> str:
        return response


    sampling_interval_in_s = 30
    collector = cls(nodename='gpu_node', sampling_interval_in_s=sampling_interval_in_s)
    assert collector.sampling_interval_in_s == sampling_interval_in_s

    collector.parse_response(response)

    monkeypatch.setattr(cls, "send_request", mock_send_request)

    collector.add_observer(observer)
    collector.start()
    time.sleep(2)
    collector.stop()

    assert observer._samples.qsize() == gpu_count
    for i in range(0,gpu_count):
        sample = observer._samples.get()
        assert type(sample) == GPUStatus

def test_GPUStatusCollector_StressTest(db_settings, monkeypatch):
    db = SlurmMonitorDB(db_settings=db_settings)

    gpu_pool = GPUStatusCollectorPool(db=db, name='gpu status')

    save_orig = gpu_pool.save
    def gpu_pool_save(sample) -> str:
        save_orig(sample)

    monkeypatch.setattr(gpu_pool, "save", gpu_pool_save)

    def nvidia_send_request(self, nodename: str, user: str) -> str:
        return '\n'.join(NVIDIA_SMI_RESPONSE)

    def rocm_send_request(self, nodename: str, user: str) -> str:
        return '\n'.join(ROCM_SMI_RESPONSE_V2)

    def hl_send_request(self, nodename: str, user: str) -> str:
        return '\n'.join(HL_SMI_RESPONSE)

    sampling_interval_in_s = 1
    for cls, request_fn in [(NvidiaInfoCollector, nvidia_send_request),
            (HabanaInfoCollector, hl_send_request),
            (ROCMInfoCollector, rocm_send_request)]:

        monkeypatch.setattr(cls, "send_request", request_fn)
        for i in range(0,200):
            collector = cls(nodename='gpu_node', sampling_interval_in_s=sampling_interval_in_s)
            assert collector.sampling_interval_in_s == sampling_interval_in_s

            gpu_pool.add_collector(collector)
            break
        break

    gpu_pool.start(verbose=False)
    # allow collection to go ahead
    time.sleep(5)
    seconds_to_run = 2

    start_time = utcnow()
    print(f"Running for {seconds_to_run} seconds -- from {utcnow()}\n")
    while (utcnow() - start_time).total_seconds() < seconds_to_run:
        results = db.fetch_all(GPUStatus)
        current_number_of_results = len(results)
        #assert current_number_of_results > number_of_results
        print(f"{(utcnow() - start_time).total_seconds():.2f} number of samples in db:"
              f" {current_number_of_results} queue_size: {gpu_pool._samples.qsize()}\r", end="")
    print("\n")

    gpu_pool.stop()
    gpu_pool.join()
    assert gpu_pool._samples.qsize() == 0
