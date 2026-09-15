from slurm_monitor.db.v2.query import Query, QueryParams


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
