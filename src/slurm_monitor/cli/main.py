from argparse import ArgumentParser
import sys
import logging
import traceback
from logging import basicConfig, getLogger

from slurm_monitor.cli.base import BaseParser
from slurm_monitor.cli.db import DBParser
from slurm_monitor.cli.probe import ProbeParser
from slurm_monitor.cli.listen import ListenParser
from slurm_monitor.cli.system_info import SystemInfoParser
from slurm_monitor.cli.autodeploy import AutoDeployParser
from slurm_monitor.cli.query import QueryParser
from slurm_monitor.cli.spec import SpecParser
from slurm_monitor.cli.data_import import ImportParser
from slurm_monitor.cli.test import TestParser

from slurm_monitor import __version__

import slurm_monitor.timescaledb.dialect #noqa
import slurm_monitor.timescaledb.functions #noqa

logger = getLogger(__name__)
logger.setLevel(logging.INFO)


class MainParser(ArgumentParser):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.description = "slurm-monitor - components for monitoring (slurm) nodes"
        self.add_argument("--log-level", type=str, default="INFO", help="Logging level")
        self.add_argument("--version", "-i", action="store_true", help="Show version")
        self.add_argument("--verbose", action="store_true", help="Show verbose information")

    def attach_subcommand_parser(
        self, subcommand: str, help: str, parser_klass: BaseParser
    ):
        if not hasattr(self, 'subparsers'):
            # lazy initialization, since it cannot be part of the __init__ function
            # otherwise random errors
            self.subparsers = self.add_subparsers(help="sub-command help")

        subparser = self.subparsers.add_parser(subcommand)
        parser_klass(parser=subparser)

def run():
    basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    main_parser = MainParser()

    main_parser.attach_subcommand_parser(
        subcommand="auto-deploy",
        help="Watch status messages and auto-deploy nodes if needed",
        parser_klass=AutoDeployParser
    )

    main_parser.attach_subcommand_parser(
        subcommand="db",
        help="Connect (create/upgrade) to database",
        parser_klass=DBParser
    )

    main_parser.attach_subcommand_parser(
        subcommand="import",
        help="Import data into the database",
        parser_klass=ImportParser
    )

    main_parser.attach_subcommand_parser(
        subcommand="listen",
        help="Listen to monitor messages",
        parser_klass=ListenParser
    )

    main_parser.attach_subcommand_parser(
        subcommand="probe",
        help="Probe/monitor system",
        parser_klass=ProbeParser
    )

    main_parser.attach_subcommand_parser(
        subcommand="query",
        help="Query the database",
        parser_klass=QueryParser
    )

    main_parser.attach_subcommand_parser(
        subcommand="spec",
        help="Validate implementation of specs",
        parser_klass=SpecParser
    )

    main_parser.attach_subcommand_parser(
        subcommand="system-info",
        help="Extract system information",
        parser_klass=SystemInfoParser
    )

    main_parser.attach_subcommand_parser(
        subcommand="test",
        help="Create a test database",
        parser_klass=TestParser
    )

    args = main_parser.parse_args()

    if args.version:
        print(__version__)
        sys.exit(0)

    if hasattr(args, "active_subparser"):
        try:
            getattr(args, "active_subparser").execute(args)
        except Exception as e:
            if args.verbose:
                traceback.print_tb(e.__traceback__)
            print(f"Error: {e}")
            sys.exit(-1)
    else:
        main_parser.print_help()

    for logger in [logging.getLogger(x) for x in logging.root.manager.loggerDict]:
        logger.setLevel(logging.getLevelName(args.log_level))

if __name__ == "__main__":
    run()
