import datetime as dt
from enum import Enum
from typing import ClassVar

from pydantic import AwareDatetime, BaseModel, ConfigDict, model_validator
from sqlalchemy import text

from slurm_monitor.db.db_base import Database


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

    statement = """
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


class GPUUtilizationAggregationLevel(str, Enum):
    CARD = "card"
    MODEL = "model"
    MANUFACTURER = "manufacturer"


class GPUUtilizationBucketLength(str, Enum):
    # corresponds to bucket length in timescaledb time buckets
    FIFTEEN_MINUTES = "15 minutes"
    HOUR = "1 hour"
    DAY = "1 day"
    WEEK = "1 week"
    MONTH = "1 month"


class GPUUtilizationLength(str, Enum):
    FIFTEEN_MINUTES = "15 minutes"
    HOUR = "1 hour"
    DAY = "1 day"
    WEEK = "1 week"
    MONTH = "1 month"
    YEAR = "1 year"


# approximate, for capping the number of buckets
_SECONDS = {
    "15 minutes": 900,
    "1 hour": 3600,
    "1 day": 86400,
    "1 week": 604800,
    "1 month": 2592000,
    "1 year": 31536000,
}
MAX_GPU_UTILIZATION_BUCKETS = 400


class UtilizationWindowParams(QueryParams):
    """Either a preset `length` up to now, or a custom `start` (and optional `end`)."""

    cluster: str
    bucket_length: GPUUtilizationBucketLength
    length: GPUUtilizationLength | None = None
    start: AwareDatetime | None = None
    end: AwareDatetime | None = None

    @model_validator(mode="after")
    def _check_window(self):
        if (self.length is None) == (self.start is None):
            raise ValueError("give either 'length' or 'start'")
        if self.start is None:
            seconds = _SECONDS[self.length.value]
        else:
            seconds = ((self.end or dt.datetime.now(dt.timezone.utc)) - self.start).total_seconds()
        if seconds <= 0:
            raise ValueError("'start' must be before 'end'")
        # keeps queries (and the frontend heatmap) small
        buckets = seconds / _SECONDS[self.bucket_length.value]
        if buckets > MAX_GPU_UTILIZATION_BUCKETS:
            raise ValueError(f"{buckets:.0f} buckets exceed the maximum of {MAX_GPU_UTILIZATION_BUCKETS}")
        return self


class GPUUtilizationParams(UtilizationWindowParams):
    level: GPUUtilizationAggregationLevel


class GPUUtilization(Query):
    parameters = GPUUtilizationParams

    # The window runs from the bucket containing `start` (or now - `length`) to `end` (or now).
    statement = """
    -- each card's latest config; configs come every ~30 min, so a day before the window finds all cards
    with cluster_cards as (
        select distinct on (uuid)
            uuid, node, index,
            -- some cards (e.g. Intel) get a new uuid every boot, node/<boot epoch>/<pci address>
            regexp_replace(uuid::text, '^([^/]+)/\\d{9,}/', '\\1/') as card
        from sysinfo_gpu_card_config
        where cluster = :cluster
          and time >= time_bucket((:bucket_length)::text::interval,
                                  coalesce((:start)::timestamptz, now() - (:length)::text::interval)) - interval '1 day'
        order by uuid, time desc
    ),
    -- aggregated before joining: joining raw samples makes the planner loop over every chunk per card
    per_card as (
        select
            time_bucket((:bucket_length)::text::interval, time) as period_start,
            uuid,
            count(*) as samples,
            sum(ce_util) as ce_util_sum,
            count(ce_util) as ce_util_count,
            -- some cards (e.g. Habana, Intel) report 0 for both power and limit
            sum(power) filter (where power_limit > 0) as power_sum,
            sum(power_limit) filter (where power_limit > 0 and power is not null) as power_limit_sum,
            count(power) filter (where power_limit > 0) as power_count
        from sample_gpu
        where time >= time_bucket((:bucket_length)::text::interval,
                                  coalesce((:start)::timestamptz, now() - (:length)::text::interval))
          and time < coalesce((:end)::timestamptz, now())
        group by 1, 2
    )
    select
        period_start,
        case (:level)::text
            when 'card' then cc.card
            when 'model' then c.model
            when 'manufacturer' then c.manufacturer
        end as gpu_group,
        -- for display only, as node/index need not be unique
        min(case (:level)::text
            when 'card' then format('%s / GPU %s', cc.node, cc.index)
            when 'model' then c.model
            when 'manufacturer' then c.manufacturer
        end) as gpu_label,
        -- for grouping rows in the frontend; a manufacturer spans several models
        case when (:level)::text <> 'manufacturer' then c.model end as model,
        c.manufacturer,
        -- distinct cards, as a card that rebooted within a bucket reports under two uuids
        count(distinct cc.card) as gpus_reporting,
        -- assumes a sample every 30s; the last bucket counts up to the window end or now (least() skips nulls)
        round(100.0 * sum(samples) * 30 / (count(distinct cc.card) * extract(epoch from
            least(period_start + (:bucket_length)::text::interval, (:end)::timestamptz, now()) - period_start
        )), 1) as percent_time_online,
        round(sum(ce_util_sum) / nullif(sum(ce_util_count), 0), 2) as avg_utilization_percent_while_online,
        round(100.0 * sum(power_sum) / nullif(sum(power_limit_sum), 0), 1) as power_percent_of_limit,
        round(sum(power_sum) / nullif(sum(power_count), 0)) as avg_power_watts,
        round(sum(power_limit_sum) / nullif(sum(power_count), 0)) as avg_power_limit_watts
    from per_card
    inner join sysinfo_gpu_card c using (uuid)
    inner join cluster_cards cc using (uuid)
    group by 1, 2, 4, 5
    order by 1, 2;
    """


class CPUUtilizationAggregationLevel(str, Enum):
    NODE = "node"
    MODEL = "model"


class CPUUtilizationParams(UtilizationWindowParams):
    level: CPUUtilizationAggregationLevel


class CPUUtilization(Query):
    parameters = CPUUtilizationParams

    # Utilization is the share of logical CPUs (hardware threads) busy, including kernel work.
    # The window runs from the bucket containing `start` (or now - `length`) to `end` (or now).
    statement = """
    -- each node's latest attributes; sysinfo comes every ~30 min, so a day before the window finds all nodes
    with cluster_nodes as (
        select distinct on (node)
            node, cpu_model,
            sockets * cores_per_socket * threads_per_core as logical_cpus,
            memory
        from sysinfo_attributes
        where cluster = :cluster
          and time >= time_bucket((:bucket_length)::text::interval,
                                  coalesce((:start)::timestamptz, now() - (:length)::text::interval)) - interval '1 day'
        order by node, time desc
    ),
    -- cpus holds each logical CPU's busy seconds since boot, so only the first and last sample
    -- of a bucket are needed: summing every sample's array is far more expensive
    per_boot as (
        select
            time_bucket((:bucket_length)::text::interval, time) as period_start,
            node,
            boot,
            count(*) as samples,
            sum(used_memory) as used_memory_sum,
            min(time) as first_time,
            max(time) as last_time,
            first(cpus, time) as first_cpus,
            last(cpus, time) as last_cpus
        from sample_system
        where cluster = :cluster
          and time >= time_bucket((:bucket_length)::text::interval,
                                  coalesce((:start)::timestamptz, now() - (:length)::text::interval))
          and time < coalesce((:end)::timestamptz, now())
        group by 1, 2, 3
    ),
    -- counters restart at boot, so a bucket continues from the previous bucket of the same boot,
    -- or from its own first sample if there is none
    per_segment as (
        select
            period_start,
            node,
            samples,
            used_memory_sum,
            last_sum - coalesce(lag(last_sum) over w, first_sum) as busy_seconds,
            extract(epoch from last_time - coalesce(lag(last_time) over w, first_time))
                * cardinality(last_cpus) as cpu_seconds
        from per_boot,
            lateral (select sum(v) as first_sum from unnest(first_cpus) as v) f,
            lateral (select sum(v) as last_sum from unnest(last_cpus) as v) l
        window w as (partition by node, boot order by period_start)
    ),
    -- a node that rebooted within a bucket has a segment per boot
    per_node as (
        select
            period_start,
            node,
            sum(samples) as samples,
            sum(used_memory_sum) as used_memory_sum,
            sum(busy_seconds) as busy_seconds,
            sum(cpu_seconds) as cpu_seconds
        from per_segment
        group by 1, 2
    )
    select
        period_start,
        case (:level)::text
            when 'node' then node
            when 'model' then cn.cpu_model
        end as cpu_group,
        cn.cpu_model as model,
        count(*) as nodes_reporting,
        sum(cn.logical_cpus) as logical_cpus,
        -- assumes a sample every 30s; the last bucket counts up to the window end or now (least() skips nulls)
        round(100.0 * sum(samples) * 30 / (count(*) * extract(epoch from
            least(period_start + (:bucket_length)::text::interval, (:end)::timestamptz, now()) - period_start
        )), 1) as percent_time_online,
        round(100.0 * sum(busy_seconds) / nullif(sum(cpu_seconds), 0), 2) as avg_utilization_percent_while_online,
        round(100.0 * sum(used_memory_sum) / nullif(sum(samples * cn.memory), 0), 1) as memory_percent_used
    from per_node
    inner join cluster_nodes cn using (node)
    group by 1, 2, 3
    order by 1, 2;
    """


class QueryMaker:
    _queries: ClassVar[dict[str, type[Query]]] = {
        "user-job-results": UserJobResults,
        "user-success-job-results": UserSuccessJobResults,
        "user-failed-job-results": UserFailedJobResults,
        "popular-partitions-by-number-of-jobs": PopularPartitionsByNumberOfJobs,
        "jobs-exceeding-resource-usage": JobsExceedingRequestedResources,
        "gpu-utilization": GPUUtilization,
        "cpu-utilization": CPUUtilization,
    }

    def create(self, db: Database, name: str) -> Query:
        if name not in self._queries:
            raise ValueError(f"QueryMaker.create: no query '{name}' exists")

        return self._queries[name](db)

    @classmethod
    def list_available(cls) -> list[str]:
        return sorted(cls._queries.keys())


class CommonQueryParams(QueryParams):
    cluster: str
    time_in_s: float | int
    interval_in_s: float | int


class ClusterQuery(Query):
    statement = """
        SELECT c.* FROM cluster_attributes c
        JOIN (
            SELECT cluster, max(time) as max_time
            FROM cluster_attributes group by cluster
        ) latest
        ON c.cluster = latest.cluster and c.time = latest.max_time;
    """


class NodesQuery(Query):
    parameters = CommonQueryParams
    statement = """
        SELECT nodes, time
        FROM cluster_attributes
        WHERE
            cluster = :cluster and
            time <= to_timestamp(:time_in_s) and
            time >= to_timestamp(:time_in_s - :interval_in_s)
        ORDER BY time DESC
        LIMIT 1
        ;
    """


class NodesPartitionsQuery(Query):
    parameters = CommonQueryParams
    statement = """
        SELECT nodes, time
        FROM cluster_attributes
        WHERE
            cluster = :cluster and
            time <= to_timestamp(:time_in_s) and
            time >= to_timestamp(:time_in_s - :interval_in_s)
        ORDER BY time DESC
        LIMIT 1
        ;
    """


class GpuNodesQuery(Query):
    parameters = CommonQueryParams
    statement = """
        SELECT cluster, node, max(time) as time
        FROM sysinfo_attributes
        WHERE
            cluster = :cluster and
            time <= to_timestamp(:time_in_s) and
            time >= to_timestamp(:time_in_s - :interval_in_s) and
            array_length(cards,1) > 0
        GROUP BY
            cluster, node
        ;
    """


class PartitionsQuery(Query):
    parameters = CommonQueryParams
    statement = """
        SELECT p.cluster, p.partition, p.nodes, p.nodes_compact, p.time
        FROM partition p
        JOIN (
            SELECT max(time) AS max_time
            FROM partition
            WHERE
                cluster = :cluster and
                time <= to_timestamp(:time_in_s) and
                time >= to_timestamp(:time_in_s - :interval_in_s)
        ) latest
        ON p.time = latest.max_time
        ;
    """
