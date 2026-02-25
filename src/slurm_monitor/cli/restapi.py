from argparse import ArgumentParser
import json
import logging
from pathlib import Path
import subprocess
import yaml

from slurm_monitor.cli.base import BaseParser
from slurm_monitor.app_settings import AppSettings
from slurm_monitor.v2 import api_v2_app

logger = logging.getLogger(__name__)


class RestapiParser(BaseParser):
    def __init__(self, parser: ArgumentParser):
        super().__init__(parser=parser)

        app_settings = AppSettings.get_instance()

        http = "https" if app_settings.ssl.keyfile else "http"
        parser.description = (
            f"slurm-monitor's RESTAPI -- Once started check the interface documentation "
            f"under {http}://{app_settings.host}:{app_settings.port}/api/v2/docs"
        )
        parser.add_argument(
            "--host",
            type=str,
            default=app_settings.host,
            help=f"Set the RESTAPI host, default is {app_settings.host}",
        )

        parser.add_argument(
            "--port",
            type=int,
            default=app_settings.port,
            help=f"Set the RESTAPI listen port, default is {app_settings.port}",
        )

        parser.add_argument(
            "--ssl-keyfile",
            type=str,
            default=app_settings.ssl.keyfile,
            help=f"Set ssl-keyfile, default is {app_settings.ssl.keyfile}",
        )

        parser.add_argument(
            "--ssl-certfile",
            type=str,
            default=app_settings.ssl.certfile,
            help=f"Set ssl-certfile, default is {app_settings.ssl.certfile}",
        )

        parser.add_argument(
            "--export-openapi",
            type=str,
            default=None,
            help="Export the current openapi schema"
        )

    def execute(self, args):
        super().execute(args)

        if args.export_openapi:
            with open(args.export_openapi, "w") as f:
                path = Path(args.export_openapi)
                if path.suffix in [".yaml", ".yml"]:
                    yaml.dump(api_v2_app.openapi(), f)
                    print(f"Exported openapi spec in 'yaml' format: {path.resolve()}")
                elif path.suffix in [".json"]:
                    json.dump(api_v2_app.openapi(), f)
                    print(f"Exported openapi spec in 'json' format: {path.resolve()}")
                else:
                    raise RuntimeError(f"Unknown export type: '{path.suffix}'")

            return


        cmd = [
            "uvicorn",
            "slurm_monitor.v2:app",
            "--port",
            str(args.port),
            "--host",
            args.host,
        ]
        # forward extra arguments to uvicorn
        cmd += self.unknown_args

        app_settings = AppSettings.get_instance()
        if app_settings.ssl.keyfile:
            cmd += ["--ssl-keyfile", app_settings.ssl.keyfile]
        if app_settings.ssl.certfile:
            cmd += ["--ssl-certfile", app_settings.ssl.certfile]

        cmd_txt = " ".join(cmd)

        logger.info(f"Execute: {cmd_txt}")
        subprocess.run(cmd_txt, shell=True)
