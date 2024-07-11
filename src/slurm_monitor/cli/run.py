from slurm_monitor.cli.base import BaseParser

from argparse import ArgumentParser
from logging import getLogger
import socket

logger = getLogger(__name__)


class RunParser(BaseParser):
    def __init__(self, parser: ArgumentParser):
        super().__init__(parser=parser)

        parser.add_argument("--port", default=10000, type=int, help="Port to use")

    def get_ip(self):
        for t in [("8.8.8.8", 1253)]:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.connect(t)
                ip = s.getsockname()[0]
                s.close()
                return ip
            except Exception as e:
                logger.warning(e)

    def execute(self, args):
        super().execute(args)

        if args.host_name is None:
            self.get_ip()


#        orchestrator = SlurmMonitor(
#            config_dir=config_dir,
#            base_dir=base_dir,
#            host_name=host_name,
#        )
