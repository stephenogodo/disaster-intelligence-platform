"""
Kafka Streaming Service

Launches the Kafka producer and consumer
as independent Python processes.
"""

from __future__ import annotations

import logging
import subprocess
import sys

from orchestration.service_status import ServiceStatus
from orchestration.services.base_service import BaseService

logger = logging.getLogger(__name__)


class KafkaService(BaseService):
    """
    Service wrapper for the Kafka streaming pipeline.
    """

    def __init__(self) -> None:

        self._status = ServiceStatus.STOPPED

        self._producer_process = None
        self._consumer_process = None

    @property
    def name(self) -> str:

        return "Kafka Streaming"

    def start(self) -> None:

        self._status = ServiceStatus.STARTING

        logger.info("Starting Kafka consumer...")

        self._consumer_process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "data_ingestion.kafka.kafka_consumer",
            ]
        )

        logger.info("Starting Kafka producer...")

        self._producer_process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "data_ingestion.kafka.kafka_producer",
            ]
        )

        self._status = ServiceStatus.RUNNING

        #
        # Wait for both processes.
        #
        self._producer_process.wait()
        self._consumer_process.wait()

    @property
    def service_type(self) -> str:
        return "stream"

    def stop(self) -> None:

        self._status = ServiceStatus.STOPPING

        if self._producer_process:

            self._producer_process.terminate()

        if self._consumer_process:

            self._consumer_process.terminate()

        self._status = ServiceStatus.STOPPED

    def status(self) -> str:

        return self._status.value