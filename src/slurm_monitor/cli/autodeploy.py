from argparse import ArgumentParser
import logging

from slurm_monitor.cli.base import BaseParser
from slurm_monitor.autodeploy import AutoDeployer
from slurm_monitor.app_settings import AppSettings

logger = logging.getLogger(__name__)

class AutoDeployParser(BaseParser):
    def __init__(self, parser: ArgumentParser):
        super().__init__(parser=parser)

    def execute(self, args):
        super().execute(args)

        AppSettings.initialize()

        deployer = AutoDeployer()
        deployer.start()

        input("Press CTRL-C to stop")

        logger.info("Shutting down ...")
        deployer.stop()
