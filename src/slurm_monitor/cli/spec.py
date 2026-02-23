from argparse import ArgumentParser
from slurm_monitor.cli.base import BaseParser

from slurm_monitor.db.v2.validation import Specification, SONAR_DEFAULT_SPEC_FILENAME
from slurm_monitor.db.v2.db_tables import TableBase
from pathlib import Path


class SpecParser(BaseParser):
    def __init__(self, parser: ArgumentParser):
        super().__init__(parser=parser)

        parser.add_argument(
            "--spec-file",
            required=False,
            type=str,
            help="YAML spec file of sonar-types",
            default=None,
        )

    def execute(self, args):
        super().execute(args)

        spec_file = None
        if args.spec_file:
            if not Path(args.spec_file).exists():
                raise FileNotFoundError(f"Could not find {args.spec_file}")
            spec_file = args.spec_file
        else:
            spec_file = Path(SONAR_DEFAULT_SPEC_FILENAME).resolve()
            print(f"Using default (inbuilt) spec file: {spec_file}")

        spec = Specification(spec_file)
        spec.validate(TableBase.metadata.tables, show=True)
