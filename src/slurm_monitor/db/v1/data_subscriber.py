from faststream import FastStream
from faststream.kafka import KafkaBroker
import aiokafka
from slurm_monitor.db.db import DatabaseSettings, SlurmMonitorDB
from slurm_monitor.db.db_tables import CPUStatus, GPUs, GPUStatus, Nodes

broker = KafkaBroker("srl-login3.ex3.simula.no:10092")
app = FastStream(broker)

gpu_status_publisher = broker.subscriber("node-status", batch=True)

db_settings = DatabaseSettings(
    user="", password="", uri=f"timescaledb://slurmuser:test@localhost:7000/ex3cluster"
)

db = SlurmMonitorDB(db_settings=db_settings)
db.insert_or_update(Nodes(
    name="n016", 
    cpu_count="256",
    cpu_model="Epyc"
    )
)
    
nodes = {}

@broker.subscriber("node-status", batch=True)
def handle_message(samples):
    global nodes
    nodes_update = {}
    for sample in samples:
        nodename = sample["node"]
        cpu_samples = [CPUStatus(**(x | {'node': nodename})) for x in sample["cpus"]]
        
        if nodename not in nodes:
            nodes_update[nodename] = Nodes(name=nodename,
                    cpu_count=len(cpu_samples),
                    cpu_model=""
            )

        gpus = {}
        gpu_samples = []
        if "gpus" in sample:
            for x in sample["gpus"]:
                uuid = x['uuid']
                gpu_status = GPUStatus(
                    uuid=uuid,
                    temperature_gpu=x['temperature_gpu'],
                    power_draw=x['power_draw'],
                    utilization_gpu=x['utilization_gpu'],
                    utilization_memory=x['utilization_memory'],
                    pstate=x['pstate'],
                    timestamp=x['timestamp']
                )
                gpu_samples.append(gpu_status)

                gpus[uuid] = GPUs(uuid=uuid,
                     node=x['node'],
                     model=x['name'],
                     local_id=x['local_id'],
                     memory_total=x['memory_total']
                )

        if nodes_update:
            db.insert_or_update([x for x in nodes_update.values()])
            nodes |= nodes_update

        db.insert_or_update(cpu_samples)
        if gpus:
            db.insert_or_update([x for x in gpus.values()])
            db.insert_or_update(gpu_samples)
