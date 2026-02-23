from argparse import ArgumentParser
from logging import getLogger
import yaml
import sys

from slurm_monitor.cli.base import BaseParser
from slurm_monitor.utils.system_info import SystemInfo

logger = getLogger(__name__)


class SystemInfoParser(BaseParser):
    def __init__(self, parser: ArgumentParser):
        super().__init__(parser=parser)

        parser.add_argument(
            "--format", default="yaml", type=str, help="Output format: yaml"
        )
        parser.add_argument(
            "-o", "--output", default=None, type=str, help="Output path"
        )
        parser.add_argument(
            "-q",
            "--query",
            default=None,
            type=str,
            help="Query field: gpus.framework | gpus.model | gpus.count",
        )

    def execute(self, args):
        super().execute(args)

        si = SystemInfo()
        # allow to access information via -q cpus.model,gpus.model
        if args.query is not None:
            response = []
            fields = args.query.split(",")
            for field in fields:
                value = None
                for label in field.split("."):
                    if value is None:
                        try:
                            value = dict(si)[label]
                        except KeyError:
                            raise RuntimeError(
                                f"{label} is not a key - "
                                f"choose from {','.join(dict(si).keys())}"
                            )
                    else:
                        if type(value) is not dict:
                            raise ValueError(f"{value} contains no dictionary")

                        try:
                            value = value[label]
                        except KeyError:
                            raise RuntimeError(
                                f"{label} is not a key - "
                                f"choose from {','.join(value.keys())}"
                            )
                response.append(str(value))
            print(",".join(response))
            sys.exit(0)

        elif args.format.lower() == "yaml":
            yaml_txt = yaml.dump(dict(si))

            if args.output is None:
                print(yaml_txt)
            else:
                with open(args.output, "w") as f:
                    f.write(yaml_txt)
