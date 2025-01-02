import json
import pytest
import subprocess
import sys
from slurm_monitor.devices import detect_gpus
from slurm_monitor.devices.gpu import GPU, GPUInfo
from slurm_monitor.utils.command import Command

from slurm_monitor.devices.nvidia import Nvidia
from slurm_monitor.devices.amd import ROCM
from slurm_monitor.devices.habana import Habana
from slurm_monitor.devices.xpu import XPU

smi_framework = {
        'nvidia': GPUInfo.Framework.CUDA,
        'rocm': GPUInfo.Framework.ROCM,
        'hl': GPUInfo.Framework.HABANA,
        'xpu': GPUInfo.Framework.XPU
}

NVIDIA_SMI_RESPONSE=[
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

# AMD ROCm System Management Interface | ROCM-SMI version: 1.4.1 | Kernel version: 6.2.4
ROCM_SMI_RESPONSE_V1DOT4 = [
    "device,Unique ID,Temperature (Sensor edge) (C),Temperature (Sensor junction) (C),Temperature (Sensor memory) (C),Temperature (Sensor HBM 0) (C),Temperature (Sensor HBM 1) (C),Temperature (Sensor HBM 2) (C),Temperature (Sensor HBM 3) (C),Average Graphics Package Power (W),GPU use (%),GFX Activity,GPU memory use (%),Memory Activity,Voltage (mV),VRAM Total Memory (B),VRAM Total Used Memory (B),Card series,Card model,Card vendor,Card SKU",
"card0,0x18f68e602b8a790f,40.0,42.0,53.0,53.0,51.0,52.0,52.0,41.0,0,2248597662,0,304605986,793,68702699520,10960896,Instinct MI210,0x0c34,Advanced Micro Devices Inc. [AMD/ATI],D67301",
"card1,0x95a1ca7691e7c391,43.0,43.0,50.0,48.0,48.0,49.0,50.0,39.0,0,1412362,0,6113,793,68702699520,10960896,Instinct MI210,0x0c34,Advanced Micro Devices Inc. [AMD/ATI],D67301"
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

ROCM_SMI_RESPONSE_V2DOT3=[
"device,Unique ID,Temperature (Sensor edge) (C),Temperature (Sensor junction) (C),Temperature (Sensor memory) (C),Average Graphics Package Power (W),GPU use (%),GPU Memory Allocated (VRAM%),GPU Memory Read/Write Activity (%),Memory Activity,Avg. Memory Bandwidth,Voltage (mV),VRAM Total Memory (B),VRAM Total Used Memory (B),Card Series,Card Model,Card Vendor,Card SKU,Subsystem ID,Device Rev,Node ID,GUID,GFX Version",
"card0,0x19645d9a01fc11bd,31.0,35.0,37.0,35.0,0,0,0,N/A,0,656,34342961152,6762496,Arcturus GL-XL [Instinct MI100],0x738c,Advanced Micro Devices Inc. [AMD/ATI],D3431401,0x0c34,0x01,8,51219,gfx9008"
]

ROCM_SMI_RESPONSE_V2DOT3DOT1=[
"device,Unique ID,Temperature (Sensor edge) (C),Temperature (Sensor junction) (C),Temperature (Sensor memory) (C),Current Socket Graphics Package Power (W),GPU use (%),GPU Memory Allocated (VRAM%),GPU Memory Read/Write Activity (%),Memory Activity,Avg. Memory Bandwidth,Voltage (mV),VRAM Total Memory (B),VRAM Total Used Memory (B),Card Series,Card Model,Card Vendor,Card SKU,Subsystem ID,Device Rev,Node ID,GUID,GFX Version",
"card0,0x19645d9a01fc11bd,31.0,35.0,37.0,35.0,0,0,0,N/A,0,656,34342961152,6762496,Arcturus GL-XL [Instinct MI100],0x738c,Advanced Micro Devices Inc. [AMD/ATI],D3431401,0x0c34,0x01,8,51219,gfx9008"
]



# Habanalabs hl-smi/hlml version hl-1.18.0-fw-53.1.1.1 (Oct 09 2024 - 20:09:06)
HL_SMI_RESPONSE=[
    "name, uuid, power.draw [W], temperature.aip [C], utilization.aip [%], memory.used [MiB], memory.total [MiB]",
    "HL-205, 00P1-HL2000A1-14-P64W99-13-05-03, 103, 36, 15, 512, 32768",
    "HL-205, 00P1-HL2000A1-14-P64W99-02-08-05, 105, 34, 15, 512, 32768",
    "HL-205, 00P1-HL2000A1-14-P64X78-02-07-10, 96, 33, 12, 512, 32768",
    "HL-205, 00P1-HL2000A1-14-P64X00-12-04-07, 100, 32, 13, 512, 32768",
    "HL-205, 00P1-HL2000A1-14-P64W98-10-03-11, 103, 34, 15, 512, 32768",
    "HL-205, 00P1-HL2000A1-14-P64X00-16-07-11, 102, 30, 14, 512, 32768",
    "HL-205, 00P1-HL2000A1-14-P64W98-05-02-04, 103, 31, 14, 512, 32768",
    "HL-205, 00P1-HL2000A1-14-P64X00-08-05-09, 103, 38, 14, 512, 32768",
]

XPU_DISCOVERY = [
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
                }]

XPU_SMI_RESPONSE = [
    "Timestamp, DeviceId, GPU Utilization (%), GPU Power (W), GPU Frequency (MHz), GPU Core Temperature (Celsius Degree), GPU Memory Temperature (Celsius Degree), GPU Memory Utilization (%), GPU Memory Used (MiB)",
"21:48:07.398,    0, 0.00, 118.87, 1550,  N/A,  N/A, 0.05, 27.99",
"21:48:07.398,    1, 0.00, 118.10, 1550,  N/A,  N/A, 0.05, 27.99"
]

@pytest.mark.parametrize("gpu_type,response,model,count,memory_total",
        [
            ["nvidia",
                [
                    "Tesla K20m, 5062",
                    "Tesla K20m, 5062"
                ],
                "Tesla K20m",2,5062
            ],
            ["rocm",
                [
                    "device,VRAM Total Memory (B),VRAM Total Used Memory (B),Card series,"
                    "Card model,Card vendor,Card SKU",
                    "card0,17163091968,10854400,"
                    "Vega 20 [Radeon Pro VII/Radeon Instinct MI50 32GB],"
                    "0x081e,Advanced Micro Devices Inc. [AMD/ATI],D1640600"
                ],
                "Vega 20 [Radeon Pro VII/Radeon Instinct MI50 32GB]",1,17163091968/1024**2
            ],
            ["hl",

                [
                    "HL-205, 32768",
                    "HL-205, 32768",
                    "HL-205, 32768",
                    "HL-205, 32768",
                    "HL-205, 32768",
                    "HL-205, 32768",
                    "HL-205, 32768",
                    "HL-205, 32768",
                ],
                "HL-205",8,32768
            ],
            ["xpu",
                XPU_DISCOVERY,
                "Intel(R) Data Center GPU Max 1100", 2, 51522830336/1024**2,
            ],
        ])

def test_GPUInfo_detect_x_smi(gpu_type, response, model, count, memory_total, monkeypatch, mocker):
    # Disable existing python libraries that could be use for retrieval
    try:
        def mock_nvmlInit():
            raise RuntimeError("Test: forced error")

        monkeypatch.setattr(pynvml, "nvmlInit", mock_nvmlInit)
    except NameError:
        pass

    mocker.patch.dict(sys.modules, {'pynvml': None })
    mocker.patch.dict(sys.modules, {'pyrsmi': None })

    orig_subprocess_run = subprocess.run
    def mock_subprocess_run(cmd, **kwargs):
        smi_response = response
        if cmd.startswith(f"command -v {gpu_type}"):
            return orig_subprocess_run("echo 'cmd'", **kwargs)
        elif cmd.startswith("command -v"):
            return orig_subprocess_run("command -v foobar", **kwargs)

        # XPU Specifics - since memory total can only be identified using 'discovery'
        if cmd == "xpu-smi discovery -j":
            smi_response = json.dumps(smi_response[0])
        elif cmd.startswith("xpu-smi discovery -d"):
            smi_response = json.dumps(smi_response[1])
        else:
            smi_response = '\n'.join(response)

        if cmd.startswith(f"{gpu_type}-smi"):
            return orig_subprocess_run(f"echo '{smi_response}'", **kwargs)

        return orig_subprocess_run(cmd, **kwargs)

    monkeypatch.setattr(subprocess, "run", mock_subprocess_run)

    gpu_info, gpu = detect_gpus()
    assert gpu_info.count == count
    assert gpu_info.model == model
    assert gpu_info.memory_total/1024**2 == memory_total
    assert gpu_info.framework == smi_framework[gpu_type]
    assert gpu is not None

@pytest.mark.parametrize(
        "cls,response,gpu_count",
        [
            [Nvidia, NVIDIA_SMI_RESPONSE, 16],
            [ROCM, ROCM_SMI_RESPONSE_V1, 1],
            [ROCM, ROCM_SMI_RESPONSE_V1DOT4, 2],
            [ROCM, ROCM_SMI_RESPONSE_V2, 2],
            [ROCM, ROCM_SMI_RESPONSE_V2DOT3, 1],
            [ROCM, ROCM_SMI_RESPONSE_V2DOT3DOT1, 1],
            [Habana, HL_SMI_RESPONSE, 8],
            [XPU, XPU_SMI_RESPONSE, 2],
        ]
)
def test_GPU_transform(cls: GPU, response: list[str] | dict[str, any], gpu_count: int, monkeypatch):
    if cls == XPU:
        # XPU Specifics - since memory total can only be identified using 'discovery'
        orig_command_run = Command.run
        def mock_command_run(cmd):
            if cmd == "xpu-smi discovery -j":
                return json.dumps(XPU_DISCOVERY[0])
            elif cmd.startswith("xpu-smi discovery -d"):
                return json.dumps(XPU_DISCOVERY[1])
            return orig_command_run(cmd)

        monkeypatch.setattr(Command, "run", mock_command_run)

    gpu = cls()
    gpu_status = gpu.transform('\n'.join(response))
    assert len(gpu_status) == gpu_count
    # in GB
    assert gpu_status[0].memory_total > 1024**3
