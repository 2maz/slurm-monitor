from argparse import ArgumentParser

from slurm_monitor.cli.base import BaseParser
from slurm_monitor.app_settings import AppSettings
import logging

import subprocess

logger = logging.getLogger(__name__)

class RestapiParser(BaseParser):
    def __init__(self, parser: ArgumentParser):
        super().__init__(parser=parser)

        app_settings = AppSettings.get_instance()

        http = "https" if app_settings.ssl.keyfile else "http"
        parser.description = f"slurm-monitor's RESTAPI -- Once started check the interface documentation " \
             f"under {http}://{app_settings.host}:{app_settings.port}/api/v2/docs"
        parser.add_argument("--host", type=str,
                            default=app_settings.host,
                            help=f"Set the RESTAPI host, default is {app_settings.host}"
        )

        parser.add_argument("--port", type=int,
                            default=app_settings.port,
                            help=f"Set the RESTAPI listen port, default is {app_settings.port}"
        )

        parser.add_argument("--ssl-keyfile", type=str,
                            default=app_settings.ssl.keyfile,
                            help=f"Set ssl-keyfile, default is {app_settings.ssl.keyfile}"
        )

        parser.add_argument("--ssl-certfile", type=str,
                            default=app_settings.ssl.certfile,
                            help=f"Set ssl-certfile, default is {app_settings.ssl.certfile}"
        )


    def execute(self, args):
        super().execute(args)

        cmd = ["uvicorn", "slurm_monitor.v2:app", "--port", str(args.port), "--host", args.host]
        # forward extra arguments to uvicorn
        cmd += self.unknown_args

        app_settings = AppSettings.get_instance()
        if app_settings.ssl.keyfile:
            cmd += ["--ssl-keyfile", app_settings.ssl.keyfile]
        if app_settings.ssl.certfile:
            cmd += ["--ssl-certfile", app_settings.ssl.certfile]

        cmd_txt = ' '.join(cmd)

        logger.info(f"Execute: {cmd_txt}")
        subprocess.run(cmd_txt, shell=True)
