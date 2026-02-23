from argparse import ArgumentParser
from pathlib import Path
import logging
from tqdm import tqdm

import slurm_monitor.db.v0 as v0
import slurm_monitor.db.v1 as v1

logger = logging.getLogger(__name__)


def migrate(v0_settings: v0.db.DatabaseSettings, v1_settings: v1.db.DatabaseSettings):
    v0_db = None
    try:
        v0_db = v0.db.SlurmMonitorDB(v0_settings)
    except Exception as e:
        raise RuntimeError(f"Failed to open db: {v0_settings.uri}") from e

    v1_db = None
    try:
        v1_db = v1.db.SlurmMonitorDB(v1_settings)
    except Exception as e:
        raise RuntimeError(f"Failed to open db: {v1_settings.uri}") from e

    logger.info("Loading Nodes")
    v0_nodes = v0_db.fetch_all(v0.db.Nodes)
    logger.info("Loading JobStatus")
    v0_db.fetch_all(v0.db.JobStatus)
    logger.info("Loading GPUStatus")
    # v0_gpu_status = v0_db.fetch_all(v0.db.GPUStatus)

    # for job in tqdm(v0_jobs, desc="migrating jobs"):
    #    v1_db.insert_or_update(v1.db_tables.JobStatus(**dict(job)))

    for node in tqdm(v0_nodes, desc="migrating nodes"):
        uuids = v0_db.get_gpu_uuids(node.name)
        v1_db.insert_or_update(v1_db.Nodes(name=node.name, cpu_count=0))  # unknown
        for uuid in uuids:
            uuid_gpu_status = v0_db.fetch_all(
                v0.db.GPUStatus, v0.db.GPUStatus.uuid == uuid
            )
            gpu_status = uuid_gpu_status[0]

            v1_db.insert_or_update(
                v1_db.GPUs(
                    uuid=gpu_status.uuid,
                    node=node.name,
                    model=gpu_status.name,
                    local_id=gpu_status.local_id,
                    memory_total=gpu_status.memory_total,
                )
            )

    gpu_status = v0_db.fetch_all(v0.db.GPUStatus)
    for status in tqdm(gpu_status, desc="gpu_status"):
        v1_db.insert_or_update(
            v1_db.GPUStatus(
                uuid=status.uuid,
                temperature_gpu=status.temperature_gpu,
                power_draw=status.power_draw,
                utilization_gpu=status.utilization_gpu,
                utilization_memory=status.utilization_memory,
                pstate=status.pstate,
                timestamp=status.timestamp,
            )
        )


def ensure_uri(db_uri) -> str:
    if db_uri.startswith("sqlite://") or db_uri.startswith("timescaledb://"):
        return db_uri

    path = Path(db_uri).resolve()
    if not path.exists():
        raise RuntimeError(
            "Failed to identify db type, and interpreting as path does not work either"
        )

    if path.suffix == ".sqlite":
        return f"sqlite:///{path}"

    raise RuntimeError(f"Failed to identify db type from suffix {path.suffix}")


def cli_run():
    logging.basicConfig()

    parser = ArgumentParser()
    parser.add_argument(
        "--from-db-uri", type=str, help="Source database (sqlite)", required=True
    )
    parser.add_argument(
        "--to-db-uri",
        type=str,
        help="Target database (postgres or sqlite)",
        required=True,
    )
    parser.add_argument("--log-level", type=str, default="INFO", help="Logging level")

    args, options = parser.parse_known_args()

    logger.setLevel(logging.getLevelName(args.log_level))

    from_db_uri = ensure_uri(args.from_db_uri)
    to_db_uri = ensure_uri(args.to_db_uri)

    v0_settings = v0.db.DatabaseSettings(uri=from_db_uri)
    v1_settings = v1.db.DatabaseSettings(uri=to_db_uri)

    migrate(v0_settings, v1_settings)
