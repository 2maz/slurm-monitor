import pytest
import asyncio
import datetime as dt
import os
import signal

from slurm_monitor.db.v1.data_publisher import (
        DataCollector,
        Controller,
        NodeStatus,
        NodeStatusCollector,
        HabanaInfoCollector,
        NvidiaInfoCollector,
        main,
        ROCMInfoCollector,
)
from slurm_monitor.utils.slurm import Slurm
from slurm_monitor.utils.process import JobMonitor


@pytest.fixture
def nodename() -> str:
    return "test-collector"

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
def test_NodeStatusCollector(
        cls: NodeStatusCollector, response: list[str], gpu_count: int,
        monkeypatch, mock_slurm_command_hint):
    Slurm._BIN_HINTS = [ mock_slurm_command_hint ]

    response = '\n'.join(response)

    def mock_get_gpu_status() -> str:
        return response


    sampling_interval_in_s = 30
    collector = cls(sampling_interval_in_s=sampling_interval_in_s)
    assert collector.sampling_interval_in_s == sampling_interval_in_s

    monkeypatch.setattr(collector, "get_gpu_status", mock_get_gpu_status)
    node_status = collector.get_node_status()

    assert len(node_status.gpus) == gpu_count


@pytest.fixture
def controller(nodename, monkeypatch):
    def mock_controller__init__(self,
            collector: DataCollector,
            bootstrap_servers: str,
            shutdown_event: asyncio.Event,
            **kwargs):
        assert type(bootstrap_servers) is str
        self.collector = collector
        self.hostname = nodename
        self.shutdown_event = shutdown_event

    monkeypatch.setattr(Controller, "__init__", mock_controller__init__)

    shutdown_event = asyncio.Event()
    collector = DataCollector(nodename, sampling_interval_in_s=2)
    return Controller(collector=collector,
                bootstrap_servers="localhost:10000",
                shutdown_event=shutdown_event,
                listen_interval_in_s=5)

@pytest.mark.parametrize("message",
        [
            [{}],
            [{"node": "not-this-node"}],
            [{"foo": "bar"}],
        ]
)
def test_controller_ignore_message(message, controller):
    controller.handle(message)

@pytest.mark.asyncio(loop_scope="module")
async def test_collector_collect(controller, mock_slurm_command_hint):
    Slurm._BIN_HINTS = [ mock_slurm_command_hint ]

    # using the mock scontrol script here
    active_jobs = JobMonitor.get_active_jobs()
    assert active_jobs.jobs

    messages = []
    def publish_fn(sample: NodeStatus):
        messages.append(sample)

    shutdown_event = asyncio.Event()

    collector = NodeStatusCollector(sampling_interval_in_s=2)
    collector_task = asyncio.create_task(collector.collect(shutdown_event, publish_fn))
    future = asyncio.gather(collector_task)

    await asyncio.sleep(4)
    shutdown_event.set()

    await future
    assert len(messages) >= 1
    node_status = messages[0]

    assert node_status is not None
    assert node_status.node
    assert node_status.cpus, "CPUs must not be empty"
    assert node_status.jobs, "Jobs must not be empty"
    assert node_status.memory, "Memory must not be empty"

    # Mocked process for these ids
    assert node_status.jobs[1]
    assert node_status.jobs[2]

    assert node_status.timestamp, "Timestamp must not be empty"
    assert type(node_status.timestamp) == dt.datetime

@pytest.mark.asyncio(loop_scope="module")
async def test_collector_collect_max_samples(controller, mock_slurm_command_hint):
    Slurm._BIN_HINTS = [ mock_slurm_command_hint ]

    # using the mock scontrol script here
    active_jobs = JobMonitor.get_active_jobs()
    assert active_jobs.jobs

    messages = []
    def publish_fn(sample: NodeStatus):
        messages.append(sample)

    shutdown_event = asyncio.Event()

    collector = NodeStatusCollector(sampling_interval_in_s=2)
    collector_task = asyncio.create_task(collector.collect(shutdown_event, publish_fn, max_samples=1))
    future = asyncio.gather(collector_task)

    await future
    assert len(messages) == 1
    node_status = messages[0]

    assert node_status is not None
    assert node_status.node
    assert node_status.cpus, "CPUs must not be empty"
    assert node_status.jobs, "Jobs must not be empty"
    assert node_status.memory, "Memory must not be empty"

    # Mocked process for these ids
    assert node_status.jobs[1]
    assert node_status.jobs[2]

    assert node_status.timestamp, "Timestamp must not be empty"
    assert type(node_status.timestamp) == dt.datetime


def test_controller_set_sampling_interval(controller, nodename):
    interval_in_s = 40
    for node in [nodename, f"{nodename[:4]}-.*", nodename[:4]]:
        data = {"node": nodename, "action": "set_interval", "interval_in_s": interval_in_s}
        controller.handle(data)

        assert controller.collector.sampling_interval_in_s == interval_in_s


def test_controller_shutdown(controller, mocker):
    mocked_os_kill_fn = mocker.patch("os.kill")

    data = {"node": controller.hostname, "action": "stop"}
    controller.handle(data)

    mocked_os_kill_fn.assert_called_once_with(os.getpid(), signal.SIGINT)
    assert controller.shutdown_event.is_set()

def test_main(mocker, mock_slurm_command_hint):
    Slurm._BIN_HINTS = [ mock_slurm_command_hint ]

    class MockKafkaProducer:
        def __init__(self, **kwargs):
            pass

    class MockKafkaConsumer:
        def __init__(self, **kwargs):
            pass

    mock_producer = mocker.patch("slurm_monitor.db.v1.data_publisher.KafkaProducer")
    mock_producer_instance = mock_producer.return_value

    published_messages = []
    publisher_topic = "test-publisher-topic"

    def send_side_effect(topic, sample):
        assert topic == publisher_topic
        published_messages.append(sample)


    mock_producer_instance.send.side_effect = send_side_effect

    mock_consumer = mocker.patch("slurm_monitor.db.v1.data_publisher.KafkaConsumer")
    mock_consumer_instance = mock_consumer.return_value
    mock_consumer_instance.poll.return_value = None


    try:
        asyncio.run(asyncio.wait_for(
            main(
                host="localhost",
                port="11111",
                publisher_topic=publisher_topic,
                subscriber_topic="test-subscriber-topic"
            ),
            timeout=5)
        )
    except asyncio.TimeoutError:
        pass

    assert published_messages
