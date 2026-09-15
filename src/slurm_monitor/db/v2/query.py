from typing import ClassVar

from pydantic import BaseModel, ConfigDict
from sqlalchemy import text

from slurm_monitor.db.v2.db_base import Database


class QueryParams(BaseModel):
    """Base class for query parameters.
    Define new query parameters by subclassing this class."""

    model_config = ConfigDict(extra="forbid", frozen=True)

class Query:
    """Base class for database queries.
    Subclass this class to define specific queries."""

    db: Database
    statement: ClassVar[str]
    parameters: ClassVar[type[QueryParams]] = QueryParams

    def __init__(self, db: Database):
        self.db = db

    def execute(self, params: dict[str, object]) -> list[dict[str, object]]:
        params = self._parse_params(params)
        with self.db.make_session() as session:
            result = session.execute(text(self.statement), params)
            return [dict(row._mapping) for row in result]

    async def execute_async(self, params: dict[str, object]) -> list[dict[str, object]]:
        params = self._parse_params(params)
        async with self.db.make_async_session() as session:
            result = await session.execute(text(self.statement), params)
            return [dict(row._mapping) for row in result]

    def _parse_params(self, params: dict[str, object]) -> dict[str, object]:
        """Parse and validate the query parameters against the schema."""
        return self.parameters.model_validate(params).model_dump()

class CommonUsageStatQueryParams(QueryParams):
    cluster: str

class UserJobResults(Query):
    """
    Generate a query to output:
        user_name, share_of_successful_jobs (in %),
        number_of_jobs (total), avg_time (per job),
        min_time, max_time, avg_cpu_count, avg_node_count
    """
    parameters = CommonUsageStatQueryParams

    statement = """
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
                    AND cluster = :cluster
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
    statement= """
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

    statement = """
        SELECT row_number() OVER(ORDER BY  user_name) as anon_user,
            user_name,
            COUNT(distinct job_id) AS number_of_failed_jobs,
            AVG(end_time - start_time) AS avg_time,
            MIN(end_time - start_time) AS min_time,
            MAX(end_time - start_time) AS max_time,
            CAST(AVG(requested_cpus) AS INTEGER) as avg_cpus,
            CAST(AVG(CARDINALITY(nodes)) AS INTEGER) as avg_node_count
        FROM (
            SELECT
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
    parameters = CommonUsageStatQueryParams

    statement = """
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
                    AND cluster = :cluster
                GROUP BY job_id
            )
        GROUP BY partition
        ORDER BY number_of_jobs;
    """


class JobsExceedingRequestedResources(Query):
    """
    Generate a query to output:
        job_id,
        max_cpu_util_exceeded_requested,
        avg_cpu_util_exceeded_requested,
        max_virtual_memory_exceeded_requested,
        gpus_used_exceeded_requested
    """
    parameters = CommonUsageStatQueryParams

    statement = r"""
        with job_requested_resources as (
            select distinct on (job_id)
                job_id,
                "time",
                coalesce((regexp_match(requested_resources, '\ynode=(\d+)'))[1]::int, 0) as nodes_requested,
                coalesce((regexp_match(requested_resources, '\ycpu=(\d+)'))[1]::int, 0) as cpus_requested,
                coalesce((regexp_match(requested_resources, '\ygpu=(\d+)'))[1]::int, 0) as gpus_requested,
                coalesce(
                    mem[1]::numeric * case upper(mem[2])
                        when 'K' then 1.0/1024
                        when 'M' then 1
                        when 'G' then 1024
                        when 'T' then 1024::numeric * 1024
                        when 'P' then 1024::numeric * 1024 * 1024
                        else 1
                    end,
                    0
                ) as mem_requested_mib,
                coalesce((regexp_match(requested_resources, '\ybilling=(\d+)'))[1]::int, 0) as billing,
                requested_resources
            from sample_slurm_job
            left join lateral
                regexp_match(requested_resources, '\ymem=(\d+(?:\.\d+)?)\s*([KMGTP])?', 'i')
                as mem on true
            where cluster = :cluster
            and "time" >= now() - interval '1 month'
            and job_state = 'RUNNING'
            and job_id not in (797193,797235,797236,797329,797330,797331)
            order by job_id, "time" asc
        ),
        sum_data as (
            select
                sp.job,
                r.cpus_requested,
                r.mem_requested_mib,
                r.gpus_requested,
                sp."time",
                sum(sp.resident_memory) as sum_resident_memory,
                sum(sp.virtual_memory)  as sum_virtual_memory,
                sum(sp.cpu_util)        as sum_cpu_util,
                sum(sp.num_threads)     as sum_num_threads,
                sum(sp.data_read)       as sum_data_read,
                sum(sp.data_written)    as sum_data_written,
                sum(sp.data_cancelled)  as sum_data_cancelled
            from sample_process sp
            join job_requested_resources r on sp.job = r.job_id
            where sp.cluster = :cluster
            and sp."time" >= now() - interval '1 hour'
            group by sp.job, r.cpus_requested, r.mem_requested_mib, r.gpus_requested, sp."time"
        ),
        aggregated_data as (
            select
                sum_data.job as job,
                cpus_requested,
                mem_requested_mib,
                gpus_requested,
                coalesce(count(distinct uuid), 0) as gpus_used,
                count(*) as n_samples,
                max(sum_resident_memory) as max_resident_memory,
                avg(sum_resident_memory) as avg_resident_memory,
                max(sum_virtual_memory)  as max_virtual_memory,
                avg(sum_virtual_memory)  as avg_virtual_memory,
                max(sum_cpu_util)        as max_cpu_util,
                avg(sum_cpu_util)        as avg_cpu_util,
                max(sum_num_threads)     as max_num_threads,
                avg(sum_num_threads)     as avg_num_threads,
                max(sum_data_read)       as max_data_read,
                max(sum_data_written)    as max_data_written,
                max(sum_data_cancelled)  as max_data_cancelled
            from sum_data
            left join sample_process_gpu on sum_data.job = sample_process_gpu.job
            group by sum_data.job, cpus_requested, mem_requested_mib, gpus_requested
            order by sum_data.job
        )
        select
            job,
            max_cpu_util > cpus_requested*1.2 as max_cpu_util_exceeded_requested,
            avg_cpu_util > cpus_requested*1.5 as avg_cpu_util_exceeded_requested,
            max_virtual_memory > mem_requested_mib as max_virtual_memory_exceeded_requested,
            gpus_used > gpus_requested as gpus_used_exceeded_requested
        from aggregated_data
        order by job;
    """



class QueryMaker:
    _queries: ClassVar[dict[str, type[Query]]] = {
            "user-job-results": UserJobResults,
            "user-success-job-results": UserSuccessJobResults,
            "user-failed-job-results": UserFailedJobResults,
            "popular-partitions-by-number-of-jobs": PopularPartitionsByNumberOfJobs,
            "jobs-exceeding-resource-usage": JobsExceedingRequestedResources,
    }

    def create(self, db: Database, name: str) -> Query:
        if name not in self._queries:
            raise ValueError(f"QueryMaker.create: no query '{name}' exists")

        return  self._queries[name](db)

    @classmethod
    def list_available(cls) -> list[str]:
        return sorted(cls._queries.keys())
