from argparse import ArgumentParser
from pathlib import Path
from time import sleep, monotonic

import logging

from slurm_monitor.cli.base import BaseParser
from slurm_monitor.db.v2.db_testing import create_test_db
from slurm_monitor.utils.command import Command

logger = logging.getLogger(__name__)

def start_timescaledb_container(
        port: int = 7654,
        password: str = "test",
        user: str = "test",
        db_name: str = "test",
        container_name: str = "timescaledb-pytest",
        image: str = "timescale/timescaledb:latest-pg16",
        stats: bool = False
    ):
    """
    Start a throwaway TimescaleDB container for manual, local dev use (the
    `slurm-monitor test` command) and block until it's actually reachable.
    """
    uri = f"timescaledb://{user}:{password}@localhost:{port}/{db_name}"

    # Remove any stale container of the same name (e.g. left over from a
    # previous run that was killed before its teardown ran) before starting.
    Command.run_and_get_exit_code(f"docker rm -f {container_name}")

    volumes = ""
    start_postgres = ""
    if stats:
        path = Path(__file__).parent.parent / "db" / "v2" / "postgresql.conf"
        if path.exists():
            conf_dir="/var/lib/postgresql/conf"
            volumes += f" -v {path.resolve()}:{conf_dir}/postgresql.conf -e POSTGRESQL_CONF_DIR={conf_dir}"
            start_postgres = f"postgres -c \"config_file={conf_dir}/postgresql.conf\""
        else:
            raise RuntimeError(f"Could not file config file {path=}")

    cmd = f"docker run -d --rm --name {container_name} {volumes} " + \
        f"-p {port}:5432 -e POSTGRES_DB={db_name} -e POSTGRES_PASSWORD={password} -e POSTGRES_USER={user} {image}"
    if start_postgres:
        cmd += f" {start_postgres}"

    logger.info(cmd)
    Command.run(cmd)

    deadline = monotonic() + 60
    while monotonic() < deadline:
        exit_code = Command.run_and_get_exit_code(
            f"docker exec {container_name} pg_isready -q -h 127.0.0.1 -U {user}"
        )
        if exit_code == 0:
            break
        sleep(0.5)
    else:
        raise RuntimeError(f"{container_name} did not become ready within 60s")

    logger.info(f"{container_name=} is ready")
    if stats:
        Command.run(f"docker exec -it {container_name} psql -U {user} -d {db_name} -c 'CREATE EXTENSION pg_stat_statements'")
        logger.info("pg_stat_statements - enabled")

    return uri


class TestParser(BaseParser):
    def __init__(self, parser: ArgumentParser):
        super().__init__(parser=parser)

        parser.add_argument("--port",
            type=int,
            default=7777,
            help="Port under which the db shall be accessible"
        )
        parser.add_argument("--user",
            type=str,
            default="test",
            help="Database user"
        )
        parser.add_argument("--password",
            type=str,
            default="test",
            help="Database password"
        )

        parser.add_argument("--image",
            type=str,
            default="timescale/timescaledb:latest-pg17",
            help="Database image"
        )

        parser.add_argument("--container-name","--name",
            type=str,
            default="timescaledb-test",
            help="Name of the container"
        )

        parser.add_argument("--with-stats",
                action="store_true",
                default=False
        )


    def execute(self, args):
        super().execute(args)

        uri = start_timescaledb_container(
                port=args.port,
                user=args.user,
                password=args.password,
                container_name=args.container_name,
                image=args.image,
                stats=args.with_stats
        )

        create_test_db(uri)
        print(uri)
