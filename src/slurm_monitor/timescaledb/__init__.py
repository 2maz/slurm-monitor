from sqlalchemy.dialects import registry

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
