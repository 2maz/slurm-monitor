from __future__ import annotations
from slurm_monitor.db.v1.db import SlurmMonitorDB
from typing import ClassVar
import asyncio
import pandas as pd

from sqlalchemy import (
        text
)

class Query:
    statement: str = None

    async def _execute(self, db: SlurmMonitorDB, query: str, params: dict[str, any] = {}):
        async with db.make_async_session() as session:
            result = await session.execute(query, params)
            return pd.DataFrame(result.fetchall(), columns=result.keys())
            #return result.all()

    def execute(self, db: SlurmMonitorDB) -> pd.DataFrame:
        return asyncio.run(self._execute(db, text(self.statement), {}))


class UserJobResults(Query):
    """
    Generate a query to output:
        user_id, share_of_successful_jobs (in %), number_of_jobs (total), avg_time (per job), min_time, max_time
    """
    statement: str = """
        SELECT user_id,
            (COUNT
                (CASE
                    WHEN exit_code = 0 and job_state in ('COMPLETED', 'FAILED', 'CANCELLED')
                    THEN 1 END) * 100 / COUNT(*)
            ) AS share_of_successful_jobs,
            COUNT(distinct job_id) AS number_of_jobs,
            AVG(end_time - start_time) AS avg_time,
            MIN(end_time - start_time) AS min_time,
            MAX(end_time - start_time) AS max_time
        FROM job_status
        WHERE
            job_state in ('COMPLETED','CANCELLED','FAILED')
        GROUP BY user_id
        ORDER BY number_of_jobs;
    """


class QueryMaker:
    db: SlurmMonitorDB

    _queries: ClassVar[dict[str, Query]] = {
            "user-job-results": UserJobResults
    }

    def __init__(self, db: SlurmMonitorDB):
        self.db = db

    def create(self, name: str) -> Query:
        if name not in self._queries:
            raise ValueError(f"{self.__class__} .run: no query '{name}' exists")

        return  self._queries[name]()

    @classmethod
    def list_available(cls):
        return cls._queries.keys()
