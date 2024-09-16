import pytest
import asyncio

from slurm_monitor.db.v1.data_publisher import DataCollector, Controller

import os
import signal

@pytest.fixture
def nodename() -> str:
    return "test-collector"


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
    collector=DataCollector(nodename, sampling_interval_in_s=2)
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
