from slurm_monitor.devices.gpu import GPU, GPUInfo

from slurm_monitor.devices.nvidia import Nvidia
from slurm_monitor.devices.amd import ROCM
from slurm_monitor.devices.habana import Habana
from slurm_monitor.devices.xpu import XPU

import logging

logger = logging.getLogger(__name__)


def detect_gpus() -> tuple[GPUInfo, GPU]:
    for gpu_class in [Nvidia, ROCM, Habana, XPU]:
        try:
            gpu_info = gpu_class.detect()
            if gpu_info.count > 0:
                return gpu_info, gpu_class()
        except RuntimeError:
            logger.info(f"GPU {gpu_class} not available")
            pass

    return GPUInfo(), None
