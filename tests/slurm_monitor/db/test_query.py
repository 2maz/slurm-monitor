import pytest
import pandas as pd

from slurm_monitor.db.v1.query import QueryMaker

@pytest.mark.parametrize("query_name", QueryMaker.list_available())
def test_gpu_infos(query_name, test_db):
    query_maker = QueryMaker(db=test_db)
    query = query_maker.create(query_name)
    df = query.execute()

    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0

