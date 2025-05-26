from argparse import ArgumentParser
import logging

from slurm_monitor.cli.base import BaseParser
from slurm_monitor.autodeploy import AutoDeployer, AutoDeployerSonar
from slurm_monitor.app_settings import AppSettings

logger = logging.getLogger(__name__)

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

        deployer.start()

        input("Press CTRL-C to stop")

        logger.info("Shutting down ...")
        deployer.stop()
