import pytest
import json
import time
import signal

import slurm_monitor.devices.gpu as gpu

from slurm_monitor.db.v1.data_publisher import NodeStatus, NodeStatusCollector
from slurm_monitor.db.v1.db_tables import (
    CPUStatus,
    MemoryStatus,
    JobStatus,
    ProcessStatus,
    GPUs,
    GPUProcess,
    GPUProcessStatus,
)
from slurm_monitor.db.v1.data_subscriber import main
from slurm_monitor.utils.slurm import Slurm


def test_get_node_status(test_db, mocker, mock_slurm_command_hint):
    Slurm._BIN_HINTS = [mock_slurm_command_hint]

    mock_consumer = mocker.patch("slurm_monitor.db.v1.data_subscriber.KafkaConsumer")
    mock_consumer_instance = mock_consumer.return_value

    class TestMessage:
        value: bytes

        def __init__(self, sample: NodeStatus):
            data = json.dumps(sample.model_dump(), default=str)
            self.value = data.encode()

    node_status_collector = NodeStatusCollector()

    def mock_messages(self):
        time.sleep(1)

        node_status = node_status_collector.get_node_status()
        node_status.gpus = [
            gpu.GPUStatus(
                uuid="aaaa-aaaa-aaaa-aaaa",
                node=node_status.node,
                model="Nvidia A100",
                local_id=0,
                memory_total=40 * 1024**3,
                temperature_gpu=28,
                power_draw=30,
                utilization_gpu=10,
                utilization_memory=50,
                pstate=None,
                timestamp=node_status.timestamp,
            )
        ]
        node_status.gpu_processes = [
            gpu.GPUProcessStatus(
                uuid="aaaa-aaaa-aaaa-aaaa",
                pid="100",
                process_name="/home/user/ml-test/run_test.sh -- 100 200",
                utilization_sm=23,
                used_memory=10,
            )
        ]

        return iter([TestMessage(node_status)])

    mock_consumer_instance.__iter__ = mock_messages

    def timeout_fn(signum, frame):
        raise TimeoutError()

    signal.signal(signal.SIGALRM, timeout_fn)
    signal.alarm(5)

    number_of_cpu_samples = len(test_db.fetch_all(CPUStatus))
    number_of_job_status = len(test_db.fetch_all(JobStatus))
    number_of_memory_status = len(test_db.fetch_all(MemoryStatus))
    number_of_process_status = len(test_db.fetch_all(ProcessStatus))
    number_of_gpus = len(test_db.fetch_all(GPUs))
    number_of_gpu_process_stats = len(test_db.fetch_all(GPUProcessStatus))
    number_of_gpu_processes = len(test_db.fetch_all(GPUProcess))

    with pytest.raises(TimeoutError):
        main(
            host="localhost",
            port=11111,
            database=test_db,
            topic="test-subscriber-topic",
            retry_timeout_in_s=5,
        )

    # ensure that db is collecting
    # initial set of sample < current set of samples
    assert number_of_cpu_samples < len(test_db.fetch_all(CPUStatus))
    assert number_of_process_status < len(test_db.fetch_all(ProcessStatus))
    assert number_of_memory_status < len(test_db.fetch_all(MemoryStatus))
    assert number_of_gpus < len(test_db.fetch_all(GPUs))
    assert number_of_gpu_process_stats < len(test_db.fetch_all(GPUProcessStatus))

    # There should one one additional process
    assert number_of_gpu_processes + 1 == len(test_db.fetch_all(GPUProcess))

    # due to the dummy function no additional jobs will be identified
    assert number_of_job_status == len(test_db.fetch_all(JobStatus))
