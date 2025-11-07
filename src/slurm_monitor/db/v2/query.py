from __future__ import annotations
from slurm_monitor.db.v2.db_base import Database
from typing import ClassVar, Awaitable
import pandas as pd

from sqlalchemy import (
        text
)

class Query:
    statement: str = None

    _db: Database
    _parameters: dict[str, str]

    def __init__(self, db: Database, parameters: dict[str, str] = {}):
        self._db = db
        self._parameters = parameters

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


    def ensure_parameter(self, name):
        """
        Check if a parameter exist in the list of given parameters
        """
        if name  not in self._parameters:
            raise ValueError(f"Missing '{param}' as parameter in query")

        return self._parameters[name]


class UserJobResults(Query):
    """
    Generate a query to output:
        user_name, share_of_successful_jobs (in %),
        number_of_jobs (total), avg_time (per job),
        min_time, max_time, avg_cpu_count, avg_node_count
    """
    @property
    def statement(self):
        cluster = self.ensure_parameter('cluster')

        return f"""
        SELECT row_number() OVER(ORDER BY  user_name) as anon_user,
            user_name,
            (COUNT
                (CASE
                    WHEN exit_code = 0 and job_state in ('COMPLETED', 'FAILED', 'CANCELLED', 'TIMEOUT')
                    THEN 1 END) * 100 / COUNT(*)
            ) AS share_of_successful_jobs,
            COUNT(distinct job_id) AS number_of_jobs,
            AVG(end_time - start_time) AS avg_time,
            MIN(end_time - start_time) AS min_time,
            MAX(end_time - start_time) AS max_time,
            CAST(AVG(requested_cpus) AS INTEGER) as avg_cpus,
            CAST(AVG(CARDINALITY(nodes)) AS INTEGER) as avg_node_count
        FROM (select
                job_id,
                LAST(user_name, time) as user_name,
                LAST(start_time,time) as start_time,
                LAST(end_time,time) as end_time,
                LAST(exit_code,time) as exit_code,
                LAST(job_state,time) as job_state,
                LAST(requested_cpus,time) as requested_cpus,
                LAST(nodes,time) as nodes from sample_slurm_job
                WHERE
                    job_state in ('COMPLETED','CANCELLED','FAILED', 'TIMEOUT')
                    AND user_name != ''
                    AND cluster = '{cluster}'
                GROUP BY job_id
            )
        GROUP BY user_name
        ORDER BY number_of_jobs;
    """

class UserSuccessJobResults(Query):
    """
    Generate a query to output:
        user_name, share_of_successful_jobs (in %),
        number_of_jobs (total), avg_time (per job),
        min_time, max_time, avg_cpu_count, avg_node_count
    """
    statement: str = """
        SELECT row_number() OVER(ORDER BY  user_name) as anon_user,
            user_name,
            COUNT(distinct job_id) AS number_of_successful_jobs,
            AVG(end_time - start_time) AS avg_time,
            MIN(end_time - start_time) AS min_time,
            MAX(end_time - start_time) AS max_time,
            CAST(AVG(requested_cpus) AS INTEGER) as avg_cpus,
            CAST(AVG(CARDINALITY(nodes)) AS INTEGER) as avg_node_count
        FROM (select
                job_id,
                LAST(user_name, time) as user_name,
                LAST(start_time,time) as start_time,
                LAST(end_time,time) as end_time,
                LAST(exit_code,time) as exit_code,
                LAST(job_state,time) as job_state,
                LAST(requested_cpus,time) as requested_cpus,
                LAST(nodes,time) as nodes from sample_slurm_job
                WHERE
                    job_state in ('COMPLETED')
                    AND user_name != ''
                    AND exit_code = 0
                GROUP BY job_id
            )
        GROUP BY user_name
        ORDER BY number_of_successful_jobs;
    """

class UserFailedJobResults(Query):
    """
    Generate a query to output:
        user_name, share_of_successful_jobs (in %),
        number_of_jobs (total), avg_time (per job),
        min_time, max_time, avg_cpu_count, avg_node_count
    """
    statement: str = """
        SELECT row_number() OVER(ORDER BY  user_name) as anon_user,
            user_name,
            COUNT(distinct job_id) AS number_of_failed_jobs,
            AVG(end_time - start_time) AS avg_time,
            MIN(end_time - start_time) AS min_time,
            MAX(end_time - start_time) AS max_time,
            CAST(AVG(requested_cpus) AS INTEGER) as avg_cpus,
            CAST(AVG(CARDINALITY(nodes)) AS INTEGER) as avg_node_count
        FROM (select
                job_id,
                LAST(user_name, time) as user_name,
                LAST(start_time,time) as start_time,
                LAST(end_time,time) as end_time,
                LAST(exit_code,time) as exit_code,
                LAST(job_state,time) as job_state,
                LAST(requested_cpus,time) as requested_cpus,
                LAST(nodes,time) as nodes from sample_slurm_job
                WHERE
                    job_state in ('FAILED')
                    AND user_name != ''
                GROUP BY job_id
            )
        GROUP BY user_name
        ORDER BY number_of_failed_jobs;
    """


class PopularPartitionsByNumberOfJobs(Query):
    """
    Generate a query to output:
        partition, number_of_jobs (total), avg_time (per job)
    """
    @property
    def statement(self):
        cluster = self.ensure_parameter('cluster')

        return f"""
        SELECT partition,
            COUNT(distinct user_name) as user_count,
            COUNT(distinct job_id) AS number_of_jobs,
            AVG(end_time - start_time) AS avg_time,
            MIN(end_time - start_time) AS min_time,
            MAX(end_time - start_time) AS max_time,
            CAST(AVG(requested_cpus) AS INTEGER) as avg_cpus,
            CAST(AVG(CARDINALITY(nodes)) AS INTEGER) as avg_node_count
        FROM (select
                job_id,
                LAST(partition, time) as partition,
                LAST(user_name, time) as user_name,
                LAST(start_time,time) as start_time,
                LAST(end_time,time) as end_time,
                LAST(exit_code,time) as exit_code,
                LAST(job_state,time) as job_state,
                LAST(requested_cpus,time) as requested_cpus,
                LAST(nodes,time) as nodes from sample_slurm_job
                WHERE
                    job_state in ('COMPLETED','CANCELLED','FAILED')
                    AND user_name != ''
                    AND cluster = '{cluster}'
                GROUP BY job_id
            )
        GROUP BY partition
        ORDER BY number_of_jobs;
    """



class QueryMaker:
    db: Database

    _queries: ClassVar[dict[str, Query]] = {
            "user-job-results": UserJobResults,
            "user-success-job-results": UserSuccessJobResults,
            "user-failed-job-results": UserFailedJobResults,
            "popular-partitions-by-number-of-jobs": PopularPartitionsByNumberOfJobs,
    }

    def __init__(self, db: Database):
        self.db = db

    def create(self, name: str, parameters: dict[str, any] = {}) -> Query:
        if name not in self._queries:
            raise ValueError(f"{self.__class__} .run: no query '{name}' exists")

        return  self._queries[name](self.db, parameters)

    @classmethod
    def list_available(cls) -> list[str]:
        return sorted(list(cls._queries.keys()))
