from argparse import ArgumentParser

import logging
from slurm_monitor.cli.base import BaseParser
from logging import basicConfig, getLogger

from slurm_monitor.cli.run import RunParser

logger = getLogger(__name__)
logger.setLevel(logging.INFO)


class MainParser(ArgumentParser):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.description = "slurm-monitor - provide interface for monitoring slurm"

        self.subparsers = self.add_subparsers(help="sub-command help")

    def attach_subcommand_parser(
        self, subcommand: str, help: str, parser_klass: BaseParser
    ):
        parser = self.subparsers.add_parser(subcommand, help=help)
        parser_klass(parser=parser)


def run():
    basicConfig()

    main_parser = MainParser()
    main_parser.attach_subcommand_parser(
        subcommand="run", help="Run", parser_klass=RunParser
    )

    args = main_parser.parse_args()
    args.active_subparser.execute(args)


if __name__ == "__main__":
    run()
