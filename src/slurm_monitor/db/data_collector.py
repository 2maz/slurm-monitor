import subprocess
import io
from threading import Thread 
import time
import datetime as dt
import subprocess
from abc import abstractmethod

import logging

from slurm_monitor.db.db import SlurmMonitorDB, DatabaseSettings
from slurm_monitor.db.db_tables import GPUStatus, Nodes

logger = logging.getLogger(__name__)

class DataCollector:
    thread: Thread
    stop: bool = False

    sampling_interval_in_s: int
    db: SlurmMonitorDB | None

    def __init__(self, sampling_interval_in_s: int, db: SlurmMonitorDB | None):
        self.sampling_interval_in_s = sampling_interval_in_s
        self.thread = Thread(target=self._run,args=())
        if db:
            self.db = db


    @staticmethod
    def get_user():
        return subprocess.run('whoami', stdout=subprocess.PIPE).stdout.decode('utf-8').strip()

    def start(self):
        self.thread.start()

    def _run(self):
        start_time = None
        while not self.stop:
            if start_time and (dt.datetime.now() - start_time).total_seconds() < self.sampling_interval_in_s:
                time.sleep(1)
            else:
                data = self.run()
                self.save(data=data)
                start_time = dt.datetime.now()

    def save(self, data):
        print(data)
        pass


class GPUInfoCollector(DataCollector):
    nodename: str
    user: str = None
    gpu_type: str

    GPU_INFO_QUERY= {
            'nvidia-smi':
            { 
                'arg': '--query-gpu',
                # https://docs.nvidia.com/deploy/nvml-api/structnvmlUtilization__t.html#structnvmlUtilization__t
                'properties': [
                    'name',
                    'uuid',
                    'power.draw',
                    'temperature.gpu', # 
                    'utilization.gpu', # Percent of time over the past sample period during which one or more kernels was executing on the GPU. 
                    'utilization.memory', # Percent of time over the past sample period during which global (device) memory was being read or written. 
                    'memory.used',
                    'memory.free',
                    # extra
                    #'pstate',
                    ]
            },
            'rocm-smi': {
                'arg': '--query-gpu',
                'properties': [
                    'gpu_name',
                    'uuid',
                    'pstate',
                    'temperature.gpu',
                    'power.draw',
                    'utilization.gpu',
                    'utilization.memory',
                    'memory.used',
                    'memory.free',
                ]
            },
            # HABANA selective query options] -Q, --query-aip <types> display only selected information, to be used with csv format.__annotations__
            #    Can be any of the following:
            #    timestamp, name, bus_id, driver_version,
            #    temperature.aip, module_id utilization.aip, memory.total, memory.free,
            #    memory.used index, serial, uuid, power.draw
            #    ecc.errors.uncorrected.aggregate.total,
            #    ecc.errors.uncorrected.volatile.total ecc.errors.corrected.aggregate.total,
            #    ecc.errors.corrected.volatile.total ecc.errors.dram.aggregate.total,
            #    ecc.errors.dram-corrected.aggregate.total ecc.errors.dram.volatile.total,
            #    ecc.errors.dram-corrected.volatile.total ecc.mode.current, ecc.mode.pending
            #    stats.violation.power, stats.violation.thermal clocks.current.soc,
            #    clocks.max.soc, clocks.limit.soc, clocks.limit.tpc pcie.link.gen.max,
            #    pcie.link.gen.current, pcie.link.width.max pcie.link.width.current,
            #    pcie.link.speed.max, pcie.link.speed.current
            'hl-smi': {
                'arg': '--query-aip', 
                'properties': [
                    'name',
                    'uuid',
                    'power.draw',
                    'temperature.aip',
                    'utilization.aip',
                    'memory.used',
                    'memory.free',
                    # extra
                    #'memory.total',
                ]
           }
    }

    def __init__(self, 
            nodename: str,
            gpu_type: str,
            user: str = None,
            db: SlurmMonitorDB = None):
        super().__init__(
                sampling_interval_in_s=10,
                db=db
        )

        if user is None:
            self.user = self.get_user()
        self.nodename = nodename
        if self.db is not None:
            self.db.insert_or_update(Nodes(name=self.nodename))

    def run(self) -> dict[str, any]:
        response = self.send_request(nodename=self.nodename, user=self.user)
        return { self.nodename : { 'gpus': self.parse_response(response) }}

    def send_request(self, nodename: str, user: str) -> str:
        msg = f"ssh {user}@{nodename} '{self.query_cmd} {self.query_argument} {','.join(self.query_properties)}'"
        return subprocess.run(msg, shell=True, stdout=subprocess.PIPE).stdout.decode('utf-8')

    @property
    @abstractmethod
    def query_properties(self) -> list[str]:
        raise NotImplementedError("Please implement 'query_properties'")


    def parse_response(self, response: str) -> dict[str]:
        gpus = []
        for line in response.strip().split("\n"):
            gpu_data = {}
            for idx,field in enumerate(line.split(",")):
                value = field.strip()
                gpu_data[self.query_properties[idx]] = value
            gpus.append(gpu_data)
        return gpus

    def save(self, data):
        super().save(data)

        if not self.db:
            return
        
        for value in data[self.nodename]["gpus"]:
            self.db.insert(GPUStatus(
                name=value["name"],
                uuid=value["uuid"],
                node=self.nodename,
                power_draw=value["power.draw"],
                temperature_gpu=value["temperature.gpu"],
                utilization_memory=value["utilization.memory"],
                utilization_gpu=value["utilization.gpu"],
                memory_total=value["memory.used"] + value["memory.free"],
                timestamp=dt.datetime.now()))

class NvidiaInfoCollector(GPUInfoCollector):
    def __init__(self, nodename: str, user: str = None, db: SlurmMonitorDB | None = None):
        super().__init__(nodename=nodename, gpu_type='nvidia', user=user, db=db)

    @property
    def query_cmd(self):
        return 'nvidia-smi'

    @property
    def query_argument(self):
        return '--format=csv,nounits,noheader --query-gpu'

    @property
    def query_properties(self):
        return ['name',
                'uuid',
                'power.draw',
                'temperature.gpu', # 
                'utilization.gpu', # Percent of time over the past sample period during which one or more kernels was executing on the GPU. 
                'utilization.memory', # Percent of time over the past sample period during which global (device) memory was being read or written. 
                'memory.used',
                'memory.free',
                # extra
                #'pstate',
                ]

class HabanaInfoCollector(GPUInfoCollector):
    def __init__(self, nodename: str, user: str = None, db: SlurmMonitorDB | None = None):
        super().__init__(nodename=nodename, gpu_type='habana', user=user, db=db)

    @property
    def query_cmd(self):
        return 'hl-smi'

    @property
    def query_argument(self):
        return '--format=csv,nounits,noheader --query-aip'

    @property
    def query_properties(self):
        return ['name',
                'uuid',
                'power.draw',
                'temperature.aip',
                'utilization.aip',
                'memory.used',
                #'memory.free',
                # extra
                'memory.total',
                ]

    def save(self, data):
        print(data)
        if not self.db:
            return
        
        for value in data[self.nodename]["gpus"]:
            self.db.insert(GPUStatus(
                name=value["name"],
                uuid=value["uuid"],
                node=self.nodename,
                power_draw=value["power.draw"],
                temperature_gpu=value["temperature.aip"],
                utilization_memory=int(value["memory.used"])*1.0/int(value["memory.total"]),
                utilization_gpu=value["utilization.aip"],
                memory_total=value["memory.total"],
                timestamp=dt.datetime.now()))

class ROCMInfoCollector(GPUInfoCollector):
    def __init__(self, nodename: str, user: str = None, db: SlurmMonitorDB | None = None):
        super().__init__(nodename=nodename, gpu_type='amd', user=user, db=db)

    @property
    def query_cmd(self):
        return 'rocm-smi'

    @property
    def query_argument(self):
        # from https://github.com/ROCm/rocm_smi_lib/tree/master/python_smi_tools
        # showmeminfo: vram, vis_vram, gtt
        #     vram: Video RAM or graphicy memory
        #     vis_vram: visible VRAM - CPU accessible video memory
        #     gtt: Graphics Translation Table
        #     all: all of the above
        return '--showuniqueid --showuse --showmemuse --showmeminfo vram --showvoltage --showtemp --showpower --csv'


    @property
    def query_properties(self):
        return ['device',
                'Unique ID', # uuid
                'Temperature (Sensor edge)', # 'temperature.sensor_edge
                'Temperature (Senors junction)', # temperature.sensor_junction
                'Temperature (Sensor memory)', # temperature.sensor_memory
                'Temperature (Sensor HBM 0)', # temperature.sensor_hbm_0
                'Temperature (Sensor HBM 1)', # temperature.sensor_hbm_1
                'Temperature (Sensor HBM 2)', # temperature.sensor_hbm_2
                'Temperature (Sensor HBM 3)', # temperature.sensor_hbm_3
                'Average Graphics Package Power (W)', # power.draw
                'GPU use (%)', # utilization.gpu
                'GFX Activity', # utilization.gfx,
                'GPU Memory use (%)', # utilization.memory / high or low (1 or 0)
                'Memory Activity',
                'Voltage (mV)',
                'VRAM Total Memory (B)', # memory.total
                'VRAM Total Used (B)', # memory.used
                ]

    def send_request(self, nodename: str, user: str) -> str:
        msg = f"ssh {user}@{nodename} '{self.query_cmd} {self.query_argument}'"
        return subprocess.run(msg, shell=True, stdout=subprocess.PIPE).stdout.decode('utf-8')

    def parse_response(self, response: str) -> dict[str]:
        gpus = []
        for line in response.strip().split("\n")[1:]:
            gpu_data = {}
            for idx,field in enumerate(line.split(",")):
                value = field.strip()
                gpu_data[self.query_properties[idx]] = value
            gpus.append(gpu_data)
        return gpus

    def save(self, data):
        print(data)

        if not self.db:
            return
       
        for value in data[self.nodename]["gpus"]:
            self.db.insert(GPUStatus(
                name=value["device"],
                uuid=value["Unique ID"],
                node=self.nodename,
                power_draw=value["Average Graphics Package Power (W)"],
                temperature_gpu=value["Temperature (Sensor edge)"],
                utilization_memory=int(value["VRAM Total Used (B)"])*1.0/int(value["VRAM Total Memory (B)"]),
                utilization_gpu=value["GPU use (%)"],
                memory_total=value["VRAM Total Memory (B)"],
                timestamp=dt.datetime.now()))



db_settings = DatabaseSettings(
        user="",
        password="",
        uri="sqlite:////tmp/test.sqlite"
)
db = SlurmMonitorDB(db_settings=db_settings)

#gpu_info_collector = NvidiaInfoCollector(nodename="g001", db=db)
#gpu_info_collector = HabanaInfoCollector(nodename="h001", db=db)
gpu_info_collector = ROCMInfoCollector(nodename="n015", db=db)

gpu_info_collector.start()
gpu_info_collector.thread.join()
