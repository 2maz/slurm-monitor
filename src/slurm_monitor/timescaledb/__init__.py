from sqlalchemy.dialects import registry
from .functions import (
    first, # noqa
    last, # noqa
    time_bucket # noqa
)

registry.register(
    'timescaledb',
    f"{__name__}.dialect",
    'TimescaledbPsycopg2Dialect'
)
registry.register(
    'timescaledb.psycopg2',
    f"{__name__}.dialect",
    'TimescaledbPsycopg2Dialect'
)
registry.register(
    'timescaledb.asyncpg',
    f"{__name__}.dialect",
    'TimescaledbAsyncpgDialect'
)

dialect = "timescaledb"
