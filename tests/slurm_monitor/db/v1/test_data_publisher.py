import pytest
import asyncio
import datetime as dt
import os
import signal
from psutil import Process

from slurm_monitor.db.v1.data_publisher import (
        DataCollector,
        Controller,
        NodeStatus,
        NodeStatusCollector,
        main,
)
from slurm_monitor.utils.slurm import Slurm
from slurm_monitor.utils.process import JobMonitor


@pytest.fixture
def nodename() -> str:
    return "test-collector"

@pytest.mark.parametrize(
        "gpu_type",
        [
            "nvidia",
            "rocm",
            "hl",
            "xpu",
        ]
)
def test_NodeStatusCollector(gpu_type, mock_gpu, monkeypatch, mock_slurm_command_hint):
    Slurm._BIN_HINTS = [ mock_slurm_command_hint ]

    sampling_interval_in_s = 30
    collector = NodeStatusCollector(sampling_interval_in_s=sampling_interval_in_s)
    assert collector.sampling_interval_in_s == sampling_interval_in_s
    node_status = collector.get_node_status()

    assert len(node_status.gpus) == mock_gpu['gpu_count']
    assert len(node_status.gpu_processes) == mock_gpu['gpu_processes']


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
async def test_collector_collect(controller, mock_slurm_command_hint, monkeypatch):
    Slurm._BIN_HINTS = [ mock_slurm_command_hint ]

    monkeypatch.setattr(JobMonitor, "get_process", lambda p : Process(1) )

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
async def test_collector_collect_max_samples(controller, mock_slurm_command_hint, monkeypatch):
    Slurm._BIN_HINTS = [ mock_slurm_command_hint ]

    monkeypatch.setattr(JobMonitor, "get_process", lambda p : Process(1) )
    # using the mock scontrol script here
    active_jobs = JobMonitor.get_active_jobs()
    assert active_jobs.jobs

    messages = []
    def publish_fn(sample: NodeStatus):
        messages.append(sample)

    shutdown_event = asyncio.Event()

    collector = NodeStatusCollector(sampling_interval_in_s=2)
    collector_task = asyncio.create_task(collector.collect(shutdown_event, publish_fn, max_samples=3))
    future = asyncio.gather(collector_task)

    await future
    assert len(messages) == 3
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
