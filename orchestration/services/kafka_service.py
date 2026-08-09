"""
Kafka Streaming Service

Launches the Kafka producer and consumer
as independent Python processes.
"""

from __future__ import annotations

import logging
import subprocess
import sys
import time

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

    @property
    def service_type(self) -> str:

        return "stream"

    def start(self) -> None:

        self._status = ServiceStatus.STARTING

        #
        # Start Kafka Consumer
        #
        logger.info("Starting Kafka consumer...")

        self._consumer_process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "data_ingestion.kafka.kafka_consumer",
            ]
        )

        #
        # Give the consumer time to subscribe
        #
        logger.info(
            "Waiting for Kafka consumer to initialise..."
        )

        time.sleep(2)

        #
        # Start Kafka Producer
        #
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
        # Wait for the producer to finish
        #
        self._producer_process.wait()

        logger.info(
            "Kafka producer finished."
        )

        #
        # Stop the consumer
        #
        logger.info(
            "Stopping Kafka consumer..."
        )

        self._consumer_process.terminate()

        self._consumer_process.wait()

        logger.info(
            "Kafka consumer stopped."
        )

        self._status = ServiceStatus.STOPPED

    def stop(self) -> None:

        self._status = ServiceStatus.STOPPING

        if (
            self._producer_process
            and self._producer_process.poll() is None
        ):
            self._producer_process.terminate()

        if (
            self._consumer_process
            and self._consumer_process.poll() is None
        ):
            self._consumer_process.terminate()

        self._status = ServiceStatus.STOPPED

    def status(self) -> str:

        return self._status.value