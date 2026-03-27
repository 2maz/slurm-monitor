from argparse import ArgumentParser
import logging
from logging.handlers import TimedRotatingFileHandler

from slurm_monitor.cli.base import BaseParser
from slurm_monitor.autodeploy import AutoDeployer, AutoDeployerSonar
from slurm_monitor.app_settings import AppSettings
from slurm_monitor.config import (
    SLURM_MONITOR_LOG_FORMAT,
    SLURM_MONITOR_LOG_STYLE,
    SLURM_MONITOR_LOG_DATE_FORMAT
)


logger = logging.getLogger(__name__)
logger.propagate = False

class AutoDeployParser(BaseParser):
    def __init__(self, parser: ArgumentParser):
        super().__init__(parser=parser)

        parser.add_argument("--use-version",
                type=str,
                default="v2",
                help="Use this API and DB version"
        )

        parser.add_argument("--cluster-name",
                type=str,
                required=True,
                help="Provide the fully qualified cluster name"
        )

        parser.add_argument("--allow-list",
                nargs='+',
                default=None,
                help="Provide the list of nodes for which redeployment is permitted"
        )

        parser.add_argument("--log-output",
                type=str,
                default=None,
                help="Output file for the log - default: slurm-monitor.auto-deploy.<cluster-name>.log, disable logging to file by specifying 'none'",
        )

        parser.add_argument("--command",
                type=str,
                default="echo",
                help="Deploy command, which will be executed with node name as argument for redeployment"
        )


    def execute(self, args):
        super().execute(args)

        app_settings = AppSettings.initialize()
        app_settings.db_schema_version = args.use_version

        if args.use_version == "v1":
            deployer = AutoDeployer(
                    app_settings=app_settings,
                    allow_list=args.allow_list)
        else:
            deployer = AutoDeployerSonar(
                    app_settings=app_settings,
                    cluster_name=args.cluster_name,
                    deploy_command=args.command,
                    allow_list=args.allow_list)

        log_output = args.log_output
        # if log_output is None (the default), we set the default
        # log file name
        if log_output is None:
            log_output = "slurm-monitor.auto-deploy.log"
            if args.cluster_name:
                log_output = "slurm-monitor.auto-deploy" + args.cluster_name + ".log"
        elif log_output.lower() == 'none':
            log_output = None

        if log_output:
            formatter = logging.Formatter(
                fmt=SLURM_MONITOR_LOG_FORMAT,
                datefmt=SLURM_MONITOR_LOG_DATE_FORMAT,
                style=SLURM_MONITOR_LOG_STYLE
            )
            root_logger = logging.getLogger()
            root_logger.handlers.clear()

            file_handler = TimedRotatingFileHandler(log_output, when='d', interval=3, backupCount=1)
            file_handler.setLevel(logging.getLevelName(args.log_level))
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            root_logger.addHandler(file_handler)

        deployer.run()
        logger.info("Shutting down ...")
