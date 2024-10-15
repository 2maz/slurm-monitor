import json
import pytest
import subprocess

from slurm_monitor.utils.system_info import GPUInfo

smi_framework = {
        'nvidia': GPUInfo.Framework.CUDA,
        'rocm': GPUInfo.Framework.ROCM,
        'hl': GPUInfo.Framework.HABANA,
        'xpu': GPUInfo.Framework.XPU
}

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
                "Vega 20 [Radeon Pro VII/Radeon Instinct MI50 32GB]",1,17163091968/1024.0**2
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
                "HL-205",8,32768,
            ],
            ["xpu",
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
                }],
                "Intel(R) Data Center GPU Max 1100", 2, 51522830336 / 1024**2,
            ],
        ])
def test_GPUInfo_detect_x_smi(gpu_type, response, model, count, memory_total, monkeypatch):
    # Disable existing python libraries that could be use for retrieval
    try:
        def mock_nvmlInit():
            raise RuntimeError("Test: forced error")

        monkeypatch.setattr(pynvml, "nvmlInit", mock_nvmlInit)
    except NameError:
        pass

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

    gpu_info = GPUInfo.detect()
    assert gpu_info.count == count
    assert gpu_info.model == model
    assert gpu_info.memory_total == memory_total
    assert gpu_info.framework == smi_framework[gpu_type]
