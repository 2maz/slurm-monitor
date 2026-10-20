from sqlalchemy import create_engine, literal_column, select

from slurm_monitor.timescaledb import time_bucket


def test_time_bucket_with_interval_string():
    engine = create_engine("timescaledb://test:test@localhost/test")
    query = select(time_bucket("5 minutes", literal_column("time")).label("bucket"))
    sql = str(query.compile(engine))
    assert "time_bucket('5 minutes',time) AS bucket" in sql
