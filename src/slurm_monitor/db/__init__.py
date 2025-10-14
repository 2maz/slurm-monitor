from .v1.db import Database as Database
from .v1.db import DatabaseSettings as DatabaseSettings
from .v1.db import SlurmMonitorDB as SlurmMonitorDB

from .v1.db_tables import CPUStatus as CPUStatus
from .v1.db_tables import GPUStatus as GPUStatus
from .v1.db_tables import GPUs as GPUs
from .v1.db_tables import Nodes as Nodes

import slurm_monitor.timescaledb # noqa

__all__ = [ "timescaledb" ]
