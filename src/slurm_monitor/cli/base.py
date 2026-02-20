"""
Module containing the BaseParser functionality, in order to simplify the usage
of subparsers.
"""
import argparse
from abc import ABC, abstractmethod
from argparse import ArgumentParser

from logging import Logger, getLogger

logger: Logger = getLogger(__name__)


class BaseParser(ABC):
    unknown_args: argparse.Namespace | None

    @abstractmethod
    def __init__(self, parser: ArgumentParser, db_required: bool = True):
        self._db_required = db_required
        parser.add_argument(
            "--active_subparser", default=self, action="store", help=argparse.SUPPRESS
        )
        self.unknown_args = None

    def execute(self, args):
        logger.debug(f"Subparser: {args.active_subparser.__class__.__name__}")
