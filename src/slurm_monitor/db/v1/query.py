from __future__ import annotations
from slurm_monitor.db.v1.db import SlurmMonitorDB
from typing import ClassVar, Awaitable
import pandas as pd

from sqlalchemy import (
        text
)

class Query:
    statement: str = None

    _db: SlurmMonitorDB

    def __init__(self, db: SlurmMonitorDB):
        self._db = db

    def _execute(self, query: str, params: dict[str, any] = {}):
        with self._db.make_session() as session:
            result = session.execute(query, params)
            return pd.DataFrame(result.fetchall(), columns=result.keys())

    def execute(self) -> pd.DataFrame:
        return self._execute(text(self.statement), {})

    async def _execute_async(self, query: str, params: dict[str, any] = {}):
        async with self._db.make_async_session() as session:
            result = await session.execute(query, params)
            return pd.DataFrame(result.fetchall(), columns=result.keys())


    async def execute_async(self) -> Awaitable[pd.DataFrame]:
        return await self._execute_async(text(self.statement), {})


class UserJobResults(Query):
    """
    Generate a query to output:
        user_id, share_of_successful_jobs (in %),
        number_of_jobs (total), avg_time (per job),
        min_time, max_time, avg_cpu
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
            MAX(end_time - start_time) AS max_time,
            CAST(AVG(cpus) AS INTEGER) as avg_cpus,
            CAST(AVG(node_count) AS INTEGER) as avg_node_count,
            CAST(AVG(tasks) AS INTEGER) as avg_tasks
        FROM job_status
        WHERE
            job_state in ('COMPLETED','CANCELLED','FAILED')
        GROUP BY user_id
        ORDER BY number_of_jobs;
    """

class PopularPartitionsByNumberOfJobs(Query):
    """
    Generate a query to output:
        partition, number_of_jobs (total), avg_time (per job)
    """
    statement: str = """
        SELECT partition,
            COUNT(distinct user_id) as user_count,
            COUNT(distinct job_id) AS number_of_jobs,
            AVG(end_time - start_time) AS avg_time,
            MIN(end_time - start_time) AS min_time,
            MAX(end_time - start_time) AS max_time,
            CAST(AVG(cpus) AS INTEGER) as avg_cpus,
            CAST(AVG(node_count) AS INTEGER) as avg_node_count,
            CAST(AVG(tasks) AS INTEGER) as avg_tasks
        FROM job_status
        WHERE
            job_state in ('COMPLETED','CANCELLED','FAILED')
        GROUP BY partition
        ORDER BY number_of_jobs;
    """



class QueryMaker:
    db: SlurmMonitorDB

    _queries: ClassVar[dict[str, Query]] = {
            "user-job-results": UserJobResults,
            "popular-partitions-by-number-of-jobs": PopularPartitionsByNumberOfJobs,
    }

    def __init__(self, db: SlurmMonitorDB):
        self.db = db

    def create(self, name: str) -> Query:
        if name not in self._queries:
            raise ValueError(f"{self.__class__} .run: no query '{name}' exists")

        return  self._queries[name](self.db)

    @classmethod
    def list_available(cls) -> list[str]:
        return sorted(list(cls._queries.keys()))
