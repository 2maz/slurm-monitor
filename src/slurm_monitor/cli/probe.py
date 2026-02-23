import asyncio
import json
from argparse import ArgumentParser

from slurm_monitor.cli.base import BaseParser
from slurm_monitor.db.v1.data_publisher import (
    main,
    KAFKA_NODE_STATUS_TOPIC,
    KAFKA_PROBE_CONTROL_TOPIC,
    NodeStatusCollector,
)


class ProbeParser(BaseParser):
    def __init__(self, parser: ArgumentParser):
        super().__init__(parser=parser)

        parser.add_argument(
            "--one-shot",
            "-o",
            action="store_true",
            default=False,
            help="Extract one sample and dump to console",
        )

        parser.add_argument(
            "--host",
            type=str,
            default="localhost",
            required=False,
            help="Kafka broker's hostname",
        )
        parser.add_argument(
            "--port",
            type=int,
            default=10092,
            help="Port on which the kafka broker is listening",
        )
        parser.add_argument(
            "--number",
            "-n",
            type=int,
            default=None,
            help="Number of collected samples after which the probe is stopped",
        )

        parser.add_argument(
            "--publisher-topic",
            type=str,
            default=KAFKA_NODE_STATUS_TOPIC,
            help=f"Topic under which samples are published -- default {KAFKA_NODE_STATUS_TOPIC}",
        )
        parser.add_argument(
            "--subscriber-topic",
            type=str,
            default=KAFKA_PROBE_CONTROL_TOPIC,
            help=f"Topic which is subscribed for control messages -- default {KAFKA_PROBE_CONTROL_TOPIC}",
        )

    def execute(self, args):
        super().execute(args)

        if args.one_shot:
            status_collector = NodeStatusCollector()
            node_status = status_collector.get_node_status()
            print(
                json.dumps(node_status.model_dump(), indent=2, default=str), flush=True
            )
            return

        # Use asyncio.run to start the event loop and run the main coroutine
        asyncio.run(
            main(
                host=args.host,
                port=args.port,
                publisher_topic=args.publisher_topic,
                subscriber_topic=args.subscriber_topic,
                max_samples=args.number,
            )
        )
