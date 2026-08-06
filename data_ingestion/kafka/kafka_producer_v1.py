"""
===============================================================================
Kafka Producer V3
===============================================================================

Project:
    Disaster Intelligence Platform

Purpose:
    Incrementally ingest FEMA Open Data datasets and publish standardised
    events to Apache Kafka.

Responsibilities:
    - Read FEMA Open Data incrementally
    - Resume from persistent checkpoints
    - Publish records to Kafka
    - Handle retries and transient failures
    - Support graceful shutdown
    - Report runtime metrics

Author:
    TerraNova Resilience Analytics Ltd.
===============================================================================
"""

from __future__ import annotations

import json
import logging
import signal
import sys
import time

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import requests
from kafka import KafkaProducer
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from storage.config import (
    ENDPOINTS,
    PAGE_SIZE,
    REQUEST_TIMEOUT,
    SLEEP_BETWEEN_REQUESTS,
    MAX_RETRIES,
    BACKOFF_FACTOR,
    HTTP_RETRY_DELAY,
    KAFKA_BOOTSTRAP,
    KAFKA_TOPIC,
    KAFKA_SEND_TIMEOUT,
    SCHEMA_VERSION,
)

from storage.checkpoint_manager import (
    get_checkpoint,
    update_checkpoint,
)


# =============================================================================
# METRICS
# =============================================================================

@dataclass(slots=True)
class ProducerMetrics:
    """
    Runtime metrics collected during execution.
    """

    started_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    records_published: int = 0

    datasets_processed: int = 0

    datasets_failed: int = 0

    http_requests: int = 0

    http_errors: int = 0

    retries: int = 0

    @property
    def elapsed_seconds(self) -> float:

        return (
            datetime.now(timezone.utc) - self.started_at
        ).total_seconds()

    @property
    def throughput(self) -> float:

        if self.elapsed_seconds <= 0:
            return 0.0

        return self.records_published / self.elapsed_seconds


# =============================================================================
# APPLICATION
# =============================================================================

class KafkaProducerApp:
    """
    Disaster Intelligence Platform Kafka Producer.
    """

    def __init__(self) -> None:

        self.running = True

        self.logger = self._configure_logging()

        self.metrics = ProducerMetrics()

        self.session: requests.Session | None = None

        self.producer: KafkaProducer | None = None

        self._register_signal_handlers()

        self.logger.info(
            "Kafka Producer V3 initialising..."
        )

    # =========================================================================
    # PRIVATE INITIALISATION METHODS
    # =========================================================================

    def _configure_logging(self) -> logging.Logger:
        """
        Configure and return the application logger.
        """

        logger = logging.getLogger("KafkaProducerV3")

        if not logger.handlers:

            handler = logging.StreamHandler(sys.stdout)

            formatter = logging.Formatter(
                "%(asctime)s | %(levelname)-8s | %(message)s"
            )

            handler.setFormatter(formatter)

            logger.addHandler(handler)

            logger.setLevel(logging.INFO)

            logger.propagate = False

        return logger


    def _register_signal_handlers(self) -> None:
        """
        Register operating system signal handlers.
        """

        signal.signal(signal.SIGINT, self._shutdown_signal)

        signal.signal(signal.SIGTERM, self._shutdown_signal)


    def _shutdown_signal(self, signum, frame) -> None:
        """
        Handle SIGINT and SIGTERM.
        """

        self.logger.info(
            "Shutdown signal received."
        )

        self.running = False


    # =========================================================================
    # HTTP SESSION
    # =========================================================================

    def create_http_session(self) -> requests.Session:
        """
        Create a resilient HTTP session.
        """

        retry_strategy = Retry(

            total=MAX_RETRIES,

            connect=MAX_RETRIES,

            read=MAX_RETRIES,

            status=MAX_RETRIES,

            backoff_factor=BACKOFF_FACTOR,

            status_forcelist=[
                429,
                500,
                502,
                503,
                504,
            ],

            allowed_methods=frozenset(["GET"]),

            respect_retry_after_header=True,

            raise_on_status=False,
        )

        adapter = HTTPAdapter(

            max_retries=retry_strategy,

            pool_connections=10,

            pool_maxsize=20,

        )

        session = requests.Session()

        session.headers.update(

            {
                "User-Agent":
                    "TerraNova Disaster Intelligence Platform",

                "Accept":
                    "application/json",
            }

        )

        session.mount(
            "https://",
            adapter,
        )

        session.mount(
            "http://",
            adapter,
        )

        self.logger.info(
            "HTTP session established."
        )

        return session


    # =========================================================================
    # KAFKA PRODUCER
    # =========================================================================

    def create_producer(self) -> KafkaProducer:
        """
        Create the Kafka producer.
        """

        producer = KafkaProducer(

            bootstrap_servers=KAFKA_BOOTSTRAP,

            key_serializer=lambda key:
                str(key).encode("utf-8"),

            value_serializer=lambda value:
                json.dumps(
                    value,
                    default=str,
                ).encode("utf-8"),

            acks="all",

            retries=10,

            linger_ms=50,

            batch_size=32768,

            compression_type="gzip",
        )

        self.logger.info(
            "Kafka producer connected to %s",
            KAFKA_BOOTSTRAP,
        )

        return producer


    # =========================================================================
    # RESOURCE INITIALISATION
    # =========================================================================

    def initialise(self) -> None:
        """
        Initialise external resources.
        """

        if self.session is None:

            self.session = self.create_http_session()

        if self.producer is None:

            self.producer = self.create_producer()

        self.logger.info(
            "Producer initialisation complete."
        )

            # =========================================================================
    # EVENT BUILDER
    # =========================================================================

    def build_event(
        self,
        dataset: str,
        record: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Build the standard event envelope for Kafka.
        """

        metadata = ENDPOINTS[dataset]

        key_field = metadata["key_field"]

        return {

            "schema_version": SCHEMA_VERSION,

            "dataset": dataset,

            "event_timestamp": datetime.now(
                timezone.utc
            ).isoformat(),

            "record_id": record.get(key_field),

            "payload": record,

        }


    # =========================================================================
# REQUEST BUILDER
# =========================================================================

    def build_request_params(
        self,
        dataset: str,
        skip: int,
        checkpoint: str | None,
    ) -> dict[str, Any]:
        """
        Build request parameters for the FEMA Open Data API.
        """

        metadata = ENDPOINTS[dataset]

        checkpoint_field = metadata["checkpoint_field"]

        key_field = metadata["key_field"]

        params = {

            "$top": PAGE_SIZE,

            "$skip": skip,

            "$select": ",".join(
                metadata["select"]
            ),

            # Stable ordering
            "$orderby": (
                f"{checkpoint_field} asc,"
                f"{key_field} asc"
            ),
        }

        if checkpoint:

            params["$filter"] = (
                f"{checkpoint_field} gt '{checkpoint}'"
            )

            self.logger.info(
                "%s | Incremental mode | checkpoint=%s",
                dataset,
                checkpoint,
            )

        else:

            self.logger.info(
                "%s | Full ingestion",
                dataset,
            )

        return params

        # =========================================================================
    # PAYLOAD PARSER
    # =========================================================================

    def extract_records(
        self,
        payload: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """
        Extract the FEMA records from an API response.

        FEMA uses different collection names for different endpoints.
        The producer therefore discovers the first non-metadata list.
        """

        for key, value in payload.items():

            if key == "metadata":

                continue

            if isinstance(value, list):

                self.logger.info(
                    "Payload collection: %s",
                    key,
                )

                return value

        self.logger.warning(
            "No records found in payload."
        )

        return []


    # =========================================================================
    # RECORD PUBLISHER
    # =========================================================================

    def publish_record(
        self,
        dataset: str,
        record: dict[str, Any],
    ) -> None:
        """
        Publish one FEMA record to Kafka.
        """

        if self.producer is None:

            raise RuntimeError(
                "Kafka producer has not been initialised."
            )

        metadata = ENDPOINTS[dataset]

        key_field = metadata["key_field"]

        event = self.build_event(
            dataset,
            record,
        )

        future = self.producer.send(

            topic=KAFKA_TOPIC,

            key=str(
                record.get(
                    key_field,
                    "UNKNOWN",
                )
            ).encode("utf-8"),

            value=event,

        )

        future.get(
            timeout=KAFKA_SEND_TIMEOUT,
        )

        self.metrics.records_published += 1
    # =========================================================================
# DATASET STREAMER
# =========================================================================

    def stream_dataset(
        self,
        dataset: str,
    ) -> None:
        """
        Stream one FEMA dataset to Kafka.

        Workflow
        --------
        1. Read checkpoint.
        2. Request one page from FEMA.
        3. Publish records to Kafka.
        4. Continue paging until the final page.
        5. Persist the newest checkpoint.
        """

        if self.session is None:
            raise RuntimeError(
                "HTTP session has not been initialised."
            )

        if self.producer is None:
            raise RuntimeError(
                "Kafka producer has not been initialised."
            )

        metadata = ENDPOINTS[dataset]

        url = metadata["url"]

        checkpoint_field = metadata["checkpoint_field"]

        checkpoint = get_checkpoint(dataset)

        latest_checkpoint = checkpoint

        skip = 0

        total_records = 0

        self.logger.info("=" * 80)
        self.logger.info("Dataset : %s", dataset)
        self.logger.info("Checkpoint : %s", checkpoint)
        self.logger.info("=" * 80)

        while self.running:

            params = self.build_request_params(
                dataset=dataset,
                skip=skip,
                checkpoint=checkpoint,
            )

            try:

                self.metrics.http_requests += 1

                response = self.session.get(
                    url,
                    params=params,
                    timeout=REQUEST_TIMEOUT,
                )

                response.raise_for_status()

                payload = response.json()

                records = self.extract_records(payload)

                if not records:

                    self.logger.info(
                        "%s | No additional records found.",
                        dataset,
                    )

                    break

                batch_size = len(records)

                self.logger.info(
                    "%s | Processing batch of %d record(s).",
                    dataset,
                    batch_size,
                )

                for record in records:

                    if not self.running:

                        self.logger.info(
                            "Shutdown requested."
                        )

                        break

                    self.publish_record(
                        dataset,
                        record,
                    )

                    total_records += 1

                    value = record.get(
                        checkpoint_field
                    )

                    if value:

                        latest_checkpoint = value

                self.producer.flush()

                self.logger.info(
                    "%s | Total published: %d",
                    dataset,
                    total_records,
                )

                if batch_size < PAGE_SIZE:

                    self.logger.info(
                        "%s | Final page reached.",
                        dataset,
                    )

                    break

                skip += PAGE_SIZE

                time.sleep(
                    SLEEP_BETWEEN_REQUESTS
                )

            except requests.exceptions.HTTPError as exc:

                self.metrics.http_errors += 1

                status = (
                    exc.response.status_code
                    if exc.response is not None
                    else "UNKNOWN"
                )

                self.logger.warning(
                    "%s | HTTP %s",
                    dataset,
                    status,
                )

                self.metrics.retries += 1

                time.sleep(
                    HTTP_RETRY_DELAY
                )

            except requests.exceptions.Timeout:

                self.metrics.http_errors += 1
                self.metrics.retries += 1

                self.logger.warning(
                    "%s | Request timeout.",
                    dataset,
                )

                time.sleep(
                    HTTP_RETRY_DELAY
                )

            except requests.exceptions.ConnectionError:

                self.metrics.http_errors += 1
                self.metrics.retries += 1

                self.logger.warning(
                    "%s | Connection error.",
                    dataset,
                )

                time.sleep(
                    HTTP_RETRY_DELAY
                )

            except requests.exceptions.RequestException as exc:

                self.metrics.http_errors += 1

                self.logger.exception(
                    "%s | Request failed: %s",
                    dataset,
                    exc,
                )

                self.metrics.retries += 1

                time.sleep(
                    HTTP_RETRY_DELAY
                )

            except Exception as exc:

                self.logger.exception(
                    "%s | Unexpected error: %s",
                    dataset,
                    exc,
                )

                raise

        if latest_checkpoint != checkpoint:

            update_checkpoint(
                dataset,
                latest_checkpoint,
            )

            self.logger.info(
                "%s | Final checkpoint saved -> %s",
                dataset,
                latest_checkpoint,
            )

        self.logger.info(
            "%s | Streaming complete (%d records published).",
            dataset,
            total_records,
        )

        self.metrics.datasets_processed += 1
        # =========================================================================
    # PIPELINE
    # =========================================================================

    def run_pipeline(self) -> None:
        """
        Execute the complete producer pipeline.

        Each configured FEMA dataset is streamed independently.
        Failure of one dataset does not prevent the remaining
        datasets from being processed.
        """

        self.logger.info("")
        self.logger.info("=" * 80)
        self.logger.info("STARTING FEMA KAFKA PRODUCER")
        self.logger.info("=" * 80)

        self.initialise()

        for dataset in ENDPOINTS.keys():

            if not self.running:

                self.logger.info(
                    "Shutdown requested."
                )

                break

            try:

                self.logger.info("")
                self.logger.info("-" * 80)
                self.logger.info(
                    "Processing dataset: %s",
                    dataset,
                )
                self.logger.info("-" * 80)

                self.stream_dataset(
                    dataset
                )

            except KeyboardInterrupt:

                self.logger.info(
                    "Pipeline interrupted."
                )

                self.running = False

                break

            except Exception as exc:

                self.metrics.datasets_failed += 1

                self.logger.exception(
                    "%s failed: %s",
                    dataset,
                    exc,
                )

        self.logger.info("")
        self.logger.info("=" * 80)
        self.logger.info("PIPELINE COMPLETE")
        self.logger.info("=" * 80)


    # =========================================================================
    # SHUTDOWN
    # =========================================================================

    def shutdown(self) -> None:
        """
        Release all external resources.
        """

        self.logger.info(
            "Commencing shutdown..."
        )

        if self.producer is not None:

            try:

                self.producer.flush()

                self.producer.close()

                self.logger.info(
                    "Kafka producer closed."
                )

            except Exception as exc:

                self.logger.warning(
                    "Error closing producer: %s",
                    exc,
                )

        if self.session is not None:

            try:

                self.session.close()

                self.logger.info(
                    "HTTP session closed."
                )

            except Exception as exc:

                self.logger.warning(
                    "Error closing session: %s",
                    exc,
                )

        self.logger.info("")
        self.logger.info("=" * 80)
        self.logger.info("EXECUTION SUMMARY")
        self.logger.info("=" * 80)

        self.logger.info(
            "Datasets processed : %d",
            self.metrics.datasets_processed,
        )

        self.logger.info(
            "Datasets failed    : %d",
            self.metrics.datasets_failed,
        )

        self.logger.info(
            "Records published  : %d",
            self.metrics.records_published,
        )

        self.logger.info(
            "HTTP requests      : %d",
            self.metrics.http_requests,
        )

        self.logger.info(
            "HTTP errors        : %d",
            self.metrics.http_errors,
        )

        self.logger.info(
            "Retries            : %d",
            self.metrics.retries,
        )

        self.logger.info(
            "Elapsed time       : %.2f seconds",
            self.metrics.elapsed_seconds,
        )

        self.logger.info(
            "Throughput         : %.2f records/sec",
            self.metrics.throughput,
        )

        self.logger.info("=" * 80)


    # =========================================================================
    # APPLICATION
    # =========================================================================

    def run(self) -> int:
        """
        Execute the producer application.

        Returns
        -------
        int
            Process exit code.
        """

        try:

            self.run_pipeline()

            return 0

        except KeyboardInterrupt:

            self.logger.info(
                "Interrupted by user."
            )

            return 0

        except Exception as exc:

            self.logger.exception(
                "Fatal error: %s",
                exc,
            )

            return 1

        finally:

            self.shutdown()


# =============================================================================
# MAIN
# =============================================================================

def main() -> int:
    """
    Application entry point.
    """

    app = KafkaProducerApp()

    return app.run()


# =============================================================================
# PYTHON ENTRY POINT
# =============================================================================

if __name__ == "__main__":

    sys.exit(
        main()
    )
                