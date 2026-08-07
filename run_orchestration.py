"""
Disaster Intelligence Platform

Main application entry point.
"""

import logging
from orchestration.services.kafka_service import KafkaService
from orchestration.platform_runner import PlatformRunner
from orchestration.services.incremental_service import IncrementalService


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)


def main() -> int:
    """
    Start the platform.
    """

    runner = PlatformRunner()

    #runner.register_service(
       # IncrementalService()
    #)

    runner.register_service(
    KafkaService()
)

    return runner.run()


if __name__ == "__main__":
    raise SystemExit(main())