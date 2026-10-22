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


# Busy CPU seconds per node and bucket from sample_system's counters, ending in per_node. Comes after
# other CTEs, needing none of them.
_CPU_BUSY_CTES = """
    -- Times and memory only: the cpus arrays, most of the table, are read just at the buckets' edges
    -- below, as aggregating them means sorting a whole window of arrays.
    per_boot as (
        select
            time_bucket((:bucket_length)::text::interval, time) as period_start,
            node,
            boot,
            count(*) as samples,
            sum(used_memory) as used_memory_sum,
            min(time) as first_time,
            max(time) as last_time
        from sample_system
        where cluster = :cluster
          and time >= time_bucket((:bucket_length)::text::interval,
                                  coalesce((:start)::timestamptz, now() - (:length)::text::interval))
          and time < coalesce((:end)::timestamptz, now())
        group by 1, 2, 3
    ),
    -- cpus holds each logical CPU's busy seconds since boot, which restart at boot. So a bucket's
    -- busy time runs from the last sample of the previous bucket of the same boot, if any, to its own.
    edges as (
        select
            p.*,
            l.busy_total,
            l.cpus,
            lag(l.busy_total) over w as previous_busy_total,
            lag(p.last_time) over w as previous_time
        from per_boot p,
            lateral (
                select (select sum(v) from unnest(s.cpus) as v) as busy_total, cardinality(s.cpus) as cpus
                from sample_system s
                where s.cluster = :cluster and s.node = p.node and s.time = p.last_time
                -- keeps it a lookup per row: as a join, it would scan and sort the whole window
                limit 1
            ) l
        window w as (partition by p.node, p.boot order by p.period_start)
    ),
    per_segment as (
        select
            period_start,
            node,
            samples,
            used_memory_sum,
            -- else from the bucket's first sample (coalesce only looks it up when needed)
            busy_total - coalesce(previous_busy_total, (
                select sum(v)
                from sample_system s, unnest(s.cpus) as v
                where s.cluster = :cluster and s.node = edges.node and s.time = edges.first_time
            )) as busy_seconds,
            extract(epoch from last_time - coalesce(previous_time, first_time)) * cpus as cpu_seconds
        from edges
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
    statement = (
        """
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
    ),"""
        + _CPU_BUSY_CTES
        + """
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
    )


# Joins the buckets that the interval from `since` to `until` overlaps, as b, stepping like buckets below.
_OVERLAPPED_BUCKETS = """
        cross join lateral (
            select t at time zone 'UTC' as period_start
            from generate_series(
                time_bucket((:bucket_length)::text::interval, greatest({since},
                            coalesce((:start)::timestamptz, now() - (:length)::text::interval))) at time zone 'UTC',
                {until} at time zone 'UTC',
                (:bucket_length)::text::interval
            ) as t
        ) overlapped
        inner join buckets b on b.period_start = overlapped.period_start"""

# A card's identity across uuids in gpu-allocation, which counts cards: its node and PCI address. Cards get
# new uuids, e.g. one every boot (Intel: node/<boot epoch>/<pci address>) or, on ex3's n016, AMD ids that changed
# and then fell back to that form. The address's PCI domain comes with 4 or 8 digits, so it's written with 4;
# a card without an address falls back to its uuid without the boot epoch. (gpu-utilization keeps one row per
# uuid without the epoch, as its card level shows uuids.)
_CARD_KEY = """coalesce(
                {node} || '/' || regexp_replace(lower(nullif({address}, '')), '^0*([0-9a-f]{{4}}:)', '\\1'),
                regexp_replace(uuid::text, '^([^/]+)/\\d{{9,}}/', '\\1/')
            )"""


# Shared by the allocation queries, after `with`: the window's buckets, the jobs running in it, what they had
# allocated per node and bucket, and when Slurm had nodes unavailable. Jobs are sampled every ~60s, so a job
# counts until a minute after its last running sample, as end_time may be a projection. job_step '' is the job
# itself, its steps repeat its allocation. A multi-node job's resources are split evenly over its nodes.
_ALLOCATION_CTES = (
    """
    -- the buckets as time_bucket() makes them; stepping in UTC, as a day need not be 24 hours in the session time zone
    buckets as (
        select
            period_start,
            -- the last bucket ends at the window end or now (least() skips nulls)
            least(period_start + (:bucket_length)::text::interval, (:end)::timestamptz, now()) as period_end
        from (
            select t at time zone 'UTC' as period_start
            from generate_series(
                time_bucket((:bucket_length)::text::interval,
                            coalesce((:start)::timestamptz, now() - (:length)::text::interval)) at time zone 'UTC',
                coalesce((:end)::timestamptz, now()) at time zone 'UTC',
                (:bucket_length)::text::interval
            ) as t
        ) series
        where period_start < coalesce((:end)::timestamptz, now())
    ),
    jobs as (
        select
            job_id,
            started,
            ended,
            nodes,
            coalesce(substring(resources from '(?:^|,)cpu=(\\d+)')::int, 0) as cpus,
            -- the total over all nodes, also given when there are typed gres/gpu:<model>= entries
            coalesce(substring(resources from '(?:^|,)gres/gpu=(\\d+)')::int, 0) as gpus
        from (
            select
                job_id,
                coalesce(min(start_time), min(time)) as started,
                max(time) + interval '60 seconds' as ended,
                max(nodes) as nodes,
                -- a running job's allocation doesn't change, so it is parsed once per job above: parsing
                -- every sample took most of the time of a year's query
                max(allocated_resources) as resources
            from sample_slurm_job
            where cluster = :cluster
              and job_step = ''
              and job_state = 'RUNNING'
              and cardinality(nodes) > 0
              and time >= time_bucket((:bucket_length)::text::interval,
                                      coalesce((:start)::timestamptz, now() - (:length)::text::interval))
              and time < coalesce((:end)::timestamptz, now())
            group by job_id
        ) per_job
    ),
    allocation as (
        select
            b.period_start,
            n.node,
            count(*) as jobs,
            count(*) filter (where j.gpus > 0) as gpu_jobs,
            sum(j.cpus::float / cardinality(j.nodes) * o.seconds) as cpu_seconds,
            sum(j.gpus::float / cardinality(j.nodes) * o.seconds) as gpu_seconds
        from jobs j
        cross join lateral unnest(j.nodes) as n(node)
        -- the buckets the job overlaps, rather than every job against every bucket"""
    + _OVERLAPPED_BUCKETS.format(since="j.started", until="j.ended")
    + """
        cross join lateral (
            select extract(epoch from least(j.ended, b.period_end) - greatest(j.started, b.period_start)) as seconds
        ) o
        where o.seconds > 0
        group by 1, 2
    ),
    -- node states come every ~30 min, each holding until the next; the hour before the window
    -- finds the state the window starts in
    node_states as (
        select
            node,
            time as since,
            coalesce(lead(time) over (partition by node order by time), now()) as until,
            states::text[] && array['DOWN', 'DRAIN', 'FAIL', 'INVALID_REG', 'MAINTENANCE', 'NOT_RESPONDING']
                as unavailable
        from node_state
        where cluster = :cluster
          and time >= time_bucket((:bucket_length)::text::interval,
                                  coalesce((:start)::timestamptz, now() - (:length)::text::interval))
                      - interval '1 hour'
          and time < coalesce((:end)::timestamptz, now())
    ),
    unavailability as (
        select
            b.period_start,
            s.node,
            sum(extract(epoch from least(s.until, b.period_end) - greatest(s.since, b.period_start)))
                as unavailable_seconds
        from node_states s"""
    + _OVERLAPPED_BUCKETS.format(since="s.since", until="s.until")
    + """
        where s.unavailable
        group by 1, 2
    ),
    -- unavailable throughout and without jobs, e.g. drained service nodes: not capacity at all
    out_of_service as (
        select node
        from unavailability
        group by node
        having sum(unavailable_seconds)
                >= 0.999 * (select sum(extract(epoch from period_end - period_start)) from buckets)
           and node not in (select node from allocation)
    )"""
)


class GPUAllocation(Query):
    parameters = UtilizationWindowParams

    # Per node and bucket, the average number of GPUs in each layer: installed (as the monitor sees them),
    # online, allocated by Slurm, occupied by a process and busy, plus the share of time Slurm had the node
    # unavailable. Rows whose GPUs should not count as Slurm's capacity say why in not_counted.
    # The window runs from the bucket containing `start` (or now - `length`) to `end` (or now).
    statement = (
        """
    with"""
        + _ALLOCATION_CTES
        + """,
    -- Each card's node, and its first and last config in the window or the 30 days before. A card configured
    -- in those days was there when the window starts: its node may stop reporting for a while (on ex3, some
    -- did for days), which would otherwise make it look added later.
    cluster_cards as (
        select
            uuid,
            (array_agg(node order by time desc))[1] as node,
            """
        + _CARD_KEY.format(
            node="(array_agg(node order by time desc))[1]", address="(array_agg(address order by time desc))[1]"
        )
        + """ as card,
            min(time) as first_seen,
            max(time) as last_seen
        from sysinfo_gpu_card_config
        where cluster = :cluster
          and time >= time_bucket((:bucket_length)::text::interval,
                                  coalesce((:start)::timestamptz, now() - (:length)::text::interval))
                      - interval '30 days'
          and time < coalesce((:end)::timestamptz, now())
        group by uuid
    ),
    -- A card counts from its first config until 3 hours after its last, so that cards and nodes added or
    -- removed in the window count only while they were there; a card with several uuids counts once.
    card_presence as (
        select node, card, min(first_seen) as since, max(last_seen) + interval '3 hours' as until
        from cluster_cards
        group by node, card
    ),
    presence as (
        select
            b.period_start,
            p.node,
            sum(extract(epoch from least(p.until, b.period_end) - greatest(p.since, b.period_start)))
                as installed_seconds
        from card_presence p"""
        + _OVERLAPPED_BUCKETS.format(since="p.since", until="p.until")
        + """
        group by 1, 2
    ),
    -- A node with cards of several models gets them all, e.g. A100 + V100. Cards seen in samples before
    -- their sysinfo get blank entries, which are skipped: a card takes its model and manufacturer from any
    -- of its uuids (e.g. Intel cards get one per boot) that has them.
    installed as (
        select
            node,
            count(*) as gpus,
            string_agg(distinct model, ' + ') as model,
            string_agg(distinct manufacturer, ' + ') as manufacturer
        from (
            select
                cc.node,
                cc.card,
                max(nullif(c.model, '')) as model,
                max(nullif(c.manufacturer, '')) as manufacturer
            from cluster_cards cc
            left join sysinfo_gpu_card c using (uuid)
            group by cc.node, cc.card
        ) cards
        group by node
    ),
    -- aggregated before joining: joining raw samples makes the planner loop over every chunk per card
    per_card as (
        select
            time_bucket((:bucket_length)::text::interval, time) as period_start,
            uuid,
            count(*) as samples,
            sum(ce_util) as ce_util_sum
        from sample_gpu
        where time >= time_bucket((:bucket_length)::text::interval,
                                  coalesce((:start)::timestamptz, now() - (:length)::text::interval))
          and time < coalesce((:end)::timestamptz, now())
        group by 1, 2
    ),
    -- assumes a sample every 30s, a busy sample counting ce_util percent of a GPU
    card_use as (
        select
            p.period_start,
            cc.node,
            sum(p.samples) * 30 as online_seconds,
            sum(p.ce_util_sum) / 100.0 * 30 as busy_seconds
        from per_card p
        inner join cluster_cards cc using (uuid)
        group by 1, 2
    ),
    -- A card is occupied in a sample when a process runs on it, by a Slurm job if that has epoch 0. Counted
    -- bucket by bucket, each scanning only its own chunks: grouping a year's samples at once spilled to disk
    -- and took most of the time of a year's query.
    occupancy as (
        select b.period_start, o.*
        from buckets b
        cross join lateral (
            select
                node,
                count(*) filter (where in_job) * 30 as occupied_seconds,
                count(*) filter (where not in_job) * 30 as occupied_outside_slurm_seconds
            from (
                select node, uuid, time, bool_or(epoch = 0) as in_job
                from sample_process_gpu
                where cluster = :cluster
                  and time >= b.period_start
                  and time < b.period_end
                group by node, uuid, time
            ) samples
            group by node
        ) o
    ),
    -- nodes Slurm gave GPUs to in the window or the 30 days before; others' cards (e.g. on login
    -- nodes, or accelerators that are no Slurm GRES) are not Slurm's to allocate
    slurm_gpu_nodes as (
        select node from allocation where gpu_seconds > 0
        union
        select distinct unnest(nodes)
        from sample_slurm_job
        where cluster = :cluster
          and job_step = ''
          and job_state = 'RUNNING'
          and allocated_resources ~ '(^|,)gres/gpu='
          and time >= time_bucket((:bucket_length)::text::interval,
                                  coalesce((:start)::timestamptz, now() - (:length)::text::interval))
                      - interval '30 days'
          and time < time_bucket((:bucket_length)::text::interval,
                                 coalesce((:start)::timestamptz, now() - (:length)::text::interval))
    ),
    -- Nodes with cards in the window, or with GPU jobs. Cards known only from the 30 days before, without GPU
    -- jobs, have nothing to show.
    present_nodes as (
        select distinct node from presence where installed_seconds > 0
    ),
    gpu_nodes as (
        select node from present_nodes
        union
        select node from allocation where gpu_seconds > 0
    )
    select
        b.period_start,
        n.node,
        i.model,
        i.manufacturer,
        round((coalesce(pr.installed_seconds, 0) / s.seconds)::numeric, 2) as gpus_installed,
        round((coalesce(u.online_seconds, 0) / s.seconds)::numeric, 2) as gpus_online,
        round((coalesce(a.gpu_seconds, 0) / s.seconds)::numeric, 2) as gpus_allocated,
        round((coalesce(o.occupied_seconds, 0) / s.seconds)::numeric, 2) as gpus_occupied,
        round((coalesce(o.occupied_outside_slurm_seconds, 0) / s.seconds)::numeric, 2) as gpus_occupied_outside_slurm,
        round((coalesce(u.busy_seconds, 0) / s.seconds)::numeric, 2) as gpus_busy,
        round((100 * coalesce(un.unavailable_seconds, 0) / s.seconds)::numeric, 1) as percent_time_unavailable,
        coalesce(a.gpu_jobs, 0) as gpu_jobs,
        case
            when n.node in (select node from out_of_service) then 'out of service'
            when n.node not in (select node from present_nodes) then 'not monitored'
            when n.node not in (select node from slurm_gpu_nodes) then 'not a Slurm GPU node'
        end as not_counted
    from buckets b
    cross join gpu_nodes n
    cross join lateral (select extract(epoch from b.period_end - b.period_start) as seconds) s
    left join installed i on i.node = n.node
    left join presence pr on pr.period_start = b.period_start and pr.node = n.node
    left join card_use u on u.period_start = b.period_start and u.node = n.node
    left join occupancy o on o.period_start = b.period_start and o.node = n.node
    left join allocation a on a.period_start = b.period_start and a.node = n.node
    left join unavailability un on un.period_start = b.period_start and un.node = n.node
    order by 1, 2;
    """
    )


class CPUAllocation(Query):
    parameters = UtilizationWindowParams

    # Per node and bucket, the average number of logical CPUs installed, online, allocated by Slurm and busy,
    # plus the share of time Slurm had the node unavailable. Slurm counts logical CPUs too, and allocates no
    # memory on ex3. Rows whose CPUs should not count as Slurm's capacity say why in not_counted.
    # The window runs from the bucket containing `start` (or now - `length`) to `end` (or now).
    statement = (
        """
    with"""
        + _ALLOCATION_CTES
        + """,
    -- Each node's latest attributes, and its first and last sysinfo in the window or the 30 days before: like
    -- a GPU card, a node that reported in those days was there when the window starts, and counts until 3 hours
    -- after its last report (sysinfo comes every 30 min), so nodes added or removed count only while there.
    cluster_nodes as (
        select distinct on (node) node, logical_cpus, cpu_model, since, until
        from (
            select
                node,
                time,
                sockets * cores_per_socket * threads_per_core as logical_cpus,
                cpu_model,
                min(time) over (partition by node) as since,
                max(time) over (partition by node) + interval '3 hours' as until
            from sysinfo_attributes
            where cluster = :cluster
              and time >= time_bucket((:bucket_length)::text::interval,
                                      coalesce((:start)::timestamptz, now() - (:length)::text::interval))
                          - interval '30 days'
              and time < coalesce((:end)::timestamptz, now())
        ) attributes
        order by node, time desc
    ),
    presence as (
        select
            b.period_start,
            p.node,
            sum(extract(epoch from least(p.until, b.period_end) - greatest(p.since, b.period_start)))
                as installed_seconds
        from cluster_nodes p"""
        + _OVERLAPPED_BUCKETS.format(since="p.since", until="p.until")
        + """
        group by 1, 2
    ),"""
        + _CPU_BUSY_CTES
        + """,
    -- nodes reporting in the window, or with jobs (see gpu-allocation)
    present_nodes as (
        select distinct node from presence where installed_seconds > 0
    ),
    cpu_nodes as (
        select node from present_nodes
        union
        select node from allocation
    )
    select
        b.period_start,
        n.node,
        cn.cpu_model as model,
        round((coalesce(cn.logical_cpus * pr.installed_seconds, 0) / s.seconds)::numeric, 2) as cpus_installed,
        round((coalesce(p.cpu_seconds, 0) / s.seconds)::numeric, 2) as cpus_online,
        round((coalesce(a.cpu_seconds, 0) / s.seconds)::numeric, 2) as cpus_allocated,
        round((coalesce(p.busy_seconds, 0) / s.seconds)::numeric, 2) as cpus_busy,
        round((100 * coalesce(un.unavailable_seconds, 0) / s.seconds)::numeric, 1) as percent_time_unavailable,
        coalesce(a.jobs, 0) as jobs,
        case
            when n.node in (select node from out_of_service) then 'out of service'
            when n.node not in (select node from present_nodes) then 'not monitored'
        end as not_counted
    from buckets b
    cross join cpu_nodes n
    cross join lateral (select extract(epoch from b.period_end - b.period_start) as seconds) s
    left join cluster_nodes cn on cn.node = n.node
    left join presence pr on pr.period_start = b.period_start and pr.node = n.node
    left join per_node p on p.period_start = b.period_start and p.node = n.node
    left join allocation a on a.period_start = b.period_start and a.node = n.node
    left join unavailability un on un.period_start = b.period_start and un.node = n.node
    order by 1, 2;
    """
    )


class QueryMaker:
    _queries: ClassVar[dict[str, type[Query]]] = {
        "user-job-results": UserJobResults,
        "user-success-job-results": UserSuccessJobResults,
        "user-failed-job-results": UserFailedJobResults,
        "popular-partitions-by-number-of-jobs": PopularPartitionsByNumberOfJobs,
        "jobs-exceeding-resource-usage": JobsExceedingRequestedResources,
        "gpu-utilization": GPUUtilization,
        "cpu-utilization": CPUUtilization,
        "gpu-allocation": GPUAllocation,
        "cpu-allocation": CPUAllocation,
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
