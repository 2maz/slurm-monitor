from argparse import ArgumentParser
import sys
import logging
from logging import basicConfig, getLogger

from slurm_monitor.cli.base import BaseParser
from slurm_monitor.cli.probe import ProbeParser
from slurm_monitor.cli.listen import ListenParser
from slurm_monitor.cli.system_info import SystemInfoParser
from slurm_monitor.cli.autodeploy import AutoDeployParser
from slurm_monitor.cli.query import QueryParser
from slurm_monitor import __version__

logger = getLogger(__name__)
logger.setLevel(logging.INFO)


class MainParser(ArgumentParser):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.description = "slurm-monitor - components for monitoring (slurm) nodes"
        self.add_argument("--log-level", type=str, default="INFO", help="Logging level")
        self.add_argument("--version","-v", action="store_true", help="Show version")
        self.subparsers = self.add_subparsers(help="sub-command help")

    def attach_subcommand_parser(
        self, subcommand: str, help: str, parser_klass: BaseParser
    ):
        parser = self.subparsers.add_parser(subcommand, help=help)
        parser_klass(parser=parser)


def run():
    basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    main_parser = MainParser()

    main_parser.attach_subcommand_parser(
        subcommand="probe",
        help="Probe/monitor system",
        parser_klass=ProbeParser
    )
    main_parser.attach_subcommand_parser(
        subcommand="listen",
        help="Listen to monitor messages",
        parser_klass=ListenParser
    )
    main_parser.attach_subcommand_parser(
        subcommand="system-info",
        help="Extract system information",
        parser_klass=SystemInfoParser
    )
    main_parser.attach_subcommand_parser(
        subcommand="auto-deploy",
        help="Watch status messages and auto-deploy nodes if needed",
        parser_klass=AutoDeployParser
    )

    main_parser.attach_subcommand_parser(
        subcommand="query",
        help="Query the database",
        parser_klass=QueryParser
    )

    args = main_parser.parse_args()

    if args.version:
        print(__version__)
        sys.exit(0)

    if hasattr(args, "active_subparser"):
        getattr(args, "active_subparser").execute(args)

    for logger in [logging.getLogger(x) for x in logging.root.manager.loggerDict]:
        logger.setLevel(logging.getLevelName(args.log_level))

if __name__ == "__main__":
    run()
