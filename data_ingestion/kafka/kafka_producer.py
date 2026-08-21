"""
===============================================================================
Disaster Intelligence Platform
Production Kafka Producer (V4)
===============================================================================

Purpose
-------
Production-grade streaming ingestion of FEMA Open Data into Apache Kafka.

Responsibilities
----------------
1. Read FEMA Open Data incrementally.
2. Publish standardized events to Kafka.
3. Persist ingestion checkpoints.
4. Support graceful shutdown.
5. Provide resilient HTTP retries.
6. Produce detailed operational logging.

Architecture
------------
FEMA API
    │
    ▼
KafkaProducerApp
    │
    ▼
Apache Kafka
    │
    ▼
Kafka Consumer
    │
    ▼
Raw Parquet Repository

Author
------
Disaster Intelligence Platform

Version
-------
4.0
"""

from __future__ import annotations

# =============================================================================
# STANDARD LIBRARY
# =============================================================================

import json
import logging
import signal
import sys
import time

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

# =============================================================================
# THIRD-PARTY
# =============================================================================

import requests

from kafka import KafkaProducer

from requests.adapters import HTTPAdapter

from urllib3.util.retry import Retry

# =============================================================================
# PROJECT IMPORTS
# =============================================================================

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
    SCHEMA_VERSION,
    KAFKA_SEND_TIMEOUT,
)

from storage.checkpoint_manager import (
    get_cursor,
    update_cursor,
)

# =============================================================================
# PRODUCER METRICS
# =============================================================================

from dataclasses import dataclass, field
from time import perf_counter


@dataclass
class ProducerMetrics:
    """
    Runtime metrics collected during producer execution.
    """

    datasets_processed: int = 0

    datasets_failed: int = 0

    records_published: int = 0

    http_requests: int = 0

    http_errors: int = 0

    retries: int = 0

    kafka_errors: int = 0

    started_at: float = field(
        default_factory=perf_counter
    )

    @property
    def elapsed_seconds(self) -> float:
        """
        Total runtime.
        """
        return perf_counter() - self.started_at

    @property
    def throughput(self) -> float:
        """
        Records published per second.
        """
        elapsed = self.elapsed_seconds

        if elapsed <= 0:
            return 0.0

        return (
            self.records_published
            / elapsed
        )

# =============================================================================
# KAFKA PRODUCER APPLICATION
# =============================================================================

class KafkaProducerApp:
    """
    Production-grade Kafka Producer for the Disaster Intelligence Platform.

    Responsibilities
    ----------------
    • Retrieve FEMA datasets incrementally.
    • Publish records asynchronously to Kafka.
    • Maintain ingestion checkpoints.
    • Produce operational metrics.
    • Handle retries and graceful shutdown.
    """

    # =========================================================================
    # CONSTRUCTION
    # =========================================================================

    def __init__(self) -> None:

        self.running = True

        self.metrics = ProducerMetrics()

        self.logger = self.configure_logging()

        self.session: requests.Session | None = None

        self.producer: KafkaProducer | None = None

        self.register_signal_handlers()

        self.logger.info(
            "Kafka Producer V5 initialising..."
        )
    # =========================================================================
    # LOGGING
    # =========================================================================

    @staticmethod
    def configure_logging() -> logging.Logger:
        """
        Configure application logging.
        """

        logger = logging.getLogger(
            "KafkaProducerV5"
        )

        if logger.handlers:
            return logger

        logger.setLevel(logging.INFO)

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(message)s"
        )

        console = logging.StreamHandler(
            sys.stdout
        )

        console.setFormatter(
            formatter
        )

        logger.addHandler(
            console
        )

        logger.propagate = False

        return logger

    # =========================================================================
    # SIGNAL HANDLING
    # =========================================================================

    def register_signal_handlers(
        self,
        ) -> None:
        """
        Register operating-system shutdown handlers.
        """

        signal.signal(
            signal.SIGINT,
            self.shutdown_signal,
        )

        signal.signal(
            signal.SIGTERM,
            self.shutdown_signal,
        )

    def shutdown_signal(
        self,
        signum,
        frame,
        ) -> None:
        """
        Stop the producer gracefully.
        """

        self.logger.info(
            "Shutdown signal received."
        )

        self.running = False

    # =========================================================================
    # HTTP SESSION
    # =========================================================================

    def create_http_session(
        self,
        ) -> requests.Session:
        """
        Create a resilient HTTP session.
        """
        retry = Retry(

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

            allowed_methods=frozenset(
                ["GET"]
            ),

            respect_retry_after_header=True,

            raise_on_status=False,
        )

        adapter = HTTPAdapter(

            max_retries=retry,

            pool_connections=10,

            pool_maxsize=20,
        )

        session = requests.Session()

        session.headers.update(

            {
                "User-Agent":
                    "Disaster-Intelligence-Platform/5.0",

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
    # INITIALISATION
    # =========================================================================

    def initialise(
        self,
        ) -> None:
        """
        Initialise producer resources.
        """

        self.logger.info("")
        self.logger.info("=" * 80)
        self.logger.info(
            "STARTING FEMA KAFKA PRODUCER"
        )
        self.logger.info("=" * 80)

        self.session = self.create_http_session()

        self.producer = self.create_producer()

        self.logger.info(
            "Producer initialisation complete."

        )

    # =========================================================================
    # KAFKA PRODUCER
    # =========================================================================

    def create_producer(
        self,
        ) -> KafkaProducer:
        """
        Create and configure the Kafka producer.
        """

        producer = KafkaProducer(

            bootstrap_servers=KAFKA_BOOTSTRAP,

            key_serializer=lambda key: (
                key.encode("utf-8")
                if isinstance(key, str)
                else key
            ),

            value_serializer=lambda value: json.dumps(
                value,
                default=str,
            ).encode("utf-8"),

            #
            # Reliability
            #
            acks="all",

            retries=5,

            enable_idempotence=True,

            max_in_flight_requests_per_connection=5,

            #
            # Throughput
            #
            linger_ms=50,

            batch_size=64 * 1024,

            compression_type="gzip",

            request_timeout_ms=KAFKA_SEND_TIMEOUT,
        )

        self.logger.info(
            "Kafka producer connected to %s",
            KAFKA_BOOTSTRAP,
        )

        return producer

    # =========================================================================
    # EVENT BUILDER
    # =========================================================================

    def build_event(
        self,
        dataset: str,
        record: dict[str, Any],
        ) -> dict[str, Any]:
        """
        Build the standard event envelope.
        """

        return {

            "schema_version": SCHEMA_VERSION,

            "dataset": dataset,

            "ingested_at": datetime.now(
                timezone.utc
            ).isoformat(),

            "payload": record,

        }

    # =========================================================================
    # PAYLOAD EXTRACTION
    # =========================================================================

    def extract_records(
        self,
        payload: dict[str, Any],
        ) -> list[dict[str, Any]]:
        """
        Extract the first list contained in a FEMA
        API response.
        """

        for value in payload.values():

            if isinstance(
                value,
                list,
            ):

                self.logger.info(
                    "Payload collection contains %d record(s).",
                    len(value),
                )

                return value

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
        Queue one FEMA record for asynchronous
        publishing.
        """

        if self.producer is None:

            raise RuntimeError(
                "Kafka producer not initialised."
            )

        event = self.build_event(
            dataset,
            record,
        )

        #
        # Queue asynchronously.
        #
        self.producer.send(

            topic=KAFKA_TOPIC,

            key=dataset,

            value=event,

        )

        self.metrics.records_published += 1

    # =========================================================================
    # BATCH PUBLISHER
    # =========================================================================

    def publish_batch(
        self,
        dataset: str,
        records: list[dict[str, Any]],
        ) -> None:
        """
        Publish an entire batch efficiently.
        """

        if self.producer is None:

            raise RuntimeError(
                "Kafka producer not initialised."
            )

        self.logger.info(
            "%s | Publishing %d record(s).",
            dataset,
            len(records),
        )

        for record in records:

            if not self.running:
                break

            self.publish_record(
                dataset,
                record,
            )

        #
        # Flush once per batch.
        #
        self.producer.flush()

    # =========================================================================
    # REQUEST BUILDER
    # =========================================================================

    def build_request_params(
        self,
        dataset: str,
        cursor: dict[str, Any],
        skip: int,
    ) -> dict[str, Any]:
        """
        Build FEMA API request parameters.
        """

        metadata = ENDPOINTS[dataset]

        checkpoint_field = metadata["checkpoint_field"]
        key_field = metadata.get("key_field")
        params = {

            "$top": PAGE_SIZE,

            "$skip": skip,

            "$select": ",".join(
                metadata["select"]
            ),

            "$orderby": (
                f"{checkpoint_field} asc"
            ),

        }

        # Public Assistance uses gmProjectId as its reliable
        # incremental cursor. FEMA's lastRefresh filter is not
        # reliable for this endpoint.
        if dataset == "public_assistance" and cursor.get("lastKey"):

            params["$skip"] = 0

            params["$orderby"] = (
                f"{key_field} asc"
            )

            params["$filter"] = (
                f"{key_field} gt "
                f"{int(cursor['lastKey'])}"
            )

            return params

        # Disaster Summaries uses disasterNumber as its reliable
        # incremental cursor. The composite lastRefresh + id
        # filter is not accepted reliably by this endpoint.
        if dataset == "disaster_summaries" and cursor.get("lastKey"):

            params["$skip"] = 0

            params["$orderby"] = (
                f"{key_field} asc"
            )

            params["$filter"] = (
                f"{key_field} gt "
                f"{int(cursor['lastKey'])}"
            )

            return params

        if cursor["lastRefresh"]:

            if key_field and cursor.get("lastKey"):

                params["$filter"] = (
                    f"({checkpoint_field} gt "
                    f"'{cursor['lastRefresh']}') "
                    f"or "
                    f"({checkpoint_field} eq "
                    f"'{cursor['lastRefresh']}' "
                    f"and {key_field} gt "
                    f"'{cursor['lastKey']}')"
                )

                params["$orderby"] = (
                    f"{checkpoint_field} asc, "
                    f"{key_field} asc"
                )

            else:

                params["$filter"] = (
                    f"{checkpoint_field} gt "
                    f"'{cursor['lastRefresh']}'"
                )

        return params

    # =========================================================================
    # PAGE FETCHER
    # =========================================================================

    def fetch_page(
        self,
        dataset: str,
        cursor: dict[str, Any],
        skip: int,
        ) -> list[dict[str, Any]]:
        """
        Retrieve one page from FEMA.
        """

        if self.session is None:

            raise RuntimeError(
                "HTTP session not initialised."
            )

        metadata = ENDPOINTS[dataset]

        url = metadata["url"]

        params = self.build_request_params(
            dataset,
            cursor,
            skip,
        )

        self.logger.debug(
            "%s | GET %s",
            dataset,
            url,
        )

        response = self.session.get(

            url,

            params=params,

            timeout=REQUEST_TIMEOUT,

        )

        self.metrics.http_requests += 1

        response.raise_for_status()

        payload = response.json()

        return self.extract_records(
            payload
        )

    # =========================================================================
    # CURSOR MANAGEMENT
    # =========================================================================

    def update_batch_cursor(
        self,
        dataset: str,
        cursor: dict[str, Any],
        records: list[dict[str, Any]],
        ) -> dict[str, Any]:
        """
        Compute the next checkpoint after
        publishing one batch.
        """

        if not records:

            return cursor

        metadata = ENDPOINTS[dataset]

        checkpoint_field = metadata[
            "checkpoint_field"
        ]

        key_field = metadata.get(
            "key_field"
        )

        newest = records[-1]

        new_cursor = {

            "lastRefresh": newest.get(
                checkpoint_field
            ),

            "lastKey": None,

        }

        if key_field:

            value = newest.get(
                key_field
            )

            if value is not None:

                new_cursor[
                    "lastKey"
                ] = str(value)

        return new_cursor

    def save_cursor(
        self,
        dataset: str,
        cursor: dict[str, Any],
        ) -> None:
        """
        Persist checkpoint.
        """

        update_cursor(

            dataset_name=dataset,

            last_refresh=cursor[
                "lastRefresh"
            ],

            last_key=cursor[
                "lastKey"
            ],

        )

        self.logger.info(

            "%s | Cursor updated -> %s",

            dataset,

            cursor,

        )

    # =========================================================================
    # DATASET STREAMER
    # =========================================================================

    def stream_dataset(
        self,
        dataset: str,
    ) -> None:
        """
        Stream one FEMA dataset incrementally.

        Workflow
        --------
        1. Load cursor.
        2. Request one page.
        3. Publish page.
        4. Update cursor.
        5. Repeat until exhausted.
        """

        if self.session is None:
            raise RuntimeError(
                "HTTP session not initialised."
            )

        if self.producer is None:
            raise RuntimeError(
                "Kafka producer not initialised."
            )

        cursor = get_cursor(
            dataset
        )

        page = 1

        total_records = 0

        self.logger.info(
            "Starting cursor : %s",
            cursor,
        )

        while self.running:

            skip = (
                page - 1
            ) * PAGE_SIZE

            try:

                records = self.fetch_page(

                    dataset=dataset,

                    cursor=cursor,

                    skip=skip,

                )

                batch_size = len(
                    records
                )

                if batch_size == 0:

                    self.logger.info(
                        "%s | No additional records.",
                        dataset,
                    )

                    break

                self.logger.info(
                    "%s | Page %d | %d record(s)",
                    dataset,
                    page,
                    batch_size,
                )

                self.publish_batch(
                    dataset,
                    records,
                )

                total_records += (
                    batch_size
                )

                new_cursor = (
                    self.update_batch_cursor(
                        dataset,
                        cursor,
                        records,
                    )
                )

                if (
                    new_cursor
                    != cursor
                ):

                    self.save_cursor(
                        dataset,
                        new_cursor,
                    )

                    cursor = (
                        new_cursor
                    )

                self.logger.info(
                    "%s | Total published: %d",
                    dataset,
                    total_records,
                )

                if (
                    batch_size
                    < PAGE_SIZE
                ):

                    self.logger.info(
                        "%s | Final page reached.",
                        dataset,
                    )

                    break

                page += 1

                time.sleep(
                    SLEEP_BETWEEN_REQUESTS
                )

            except requests.exceptions.HTTPError as exc:

                self.metrics.http_errors += 1

                status = (
                    exc.response.status_code
                    if exc.response is not None
                    else None
                )

                #
                # Permanent client errors
                #

                if status in (
                    400,
                    401,
                    403,
                    404,
                ):

                    self.logger.error(
                        "%s | HTTP %s (non-retryable).",
                        dataset,
                        status,
                    )

                    self.metrics.datasets_failed += 1

                    break

                #
                # Retryable server errors
                #

                if status in (
                    429,
                    500,
                    502,
                    503,
                    504,
                ):

                    self.logger.warning(
                        "%s | HTTP %s",
                        dataset,
                        status,
                    )

                    self.metrics.retries += 1

                    time.sleep(
                        HTTP_RETRY_DELAY
                    )

                    continue

                raise

            except (
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError,
                requests.exceptions.ConnectionError,
            ):

                self.metrics.http_errors += 1
                self.metrics.retries += 1

                self.logger.warning(
                    "%s | Temporary connection problem.",
                    dataset,
                )

                time.sleep(
                    HTTP_RETRY_DELAY
                )

            except Exception as exc:

                self.metrics.datasets_failed += 1

                self.logger.exception(
                    "%s | Pipeline failure: %s",
                    dataset,
                    exc,
                )

                break

        self.logger.info(
            "%s | Streaming complete.",
            dataset,
        )

        self.metrics.datasets_processed += 1

    # =========================================================================
    # PIPELINE ORCHESTRATOR
    # =========================================================================

    def run_pipeline(
        self,
        ) -> None:
        """
        Execute the complete FEMA ingestion pipeline.

        Each dataset is processed independently so that a failure in one
        dataset does not stop the remaining datasets.
        """

        self.initialise()

        for dataset in ENDPOINTS:

            if not self.running:

                self.logger.info(
                    "Shutdown requested."
                )

                break

            try:

                self.logger.info("")

                self.logger.info(
                    "-" * 80
                )

                self.logger.info(
                    "Processing dataset: %s",
                    dataset,
                )

                self.logger.info(
                    "-" * 80
                )

                self.stream_dataset(
                    dataset,
                )

            except KeyboardInterrupt:

                raise

            except Exception as exc:

                self.metrics.datasets_failed += 1

                self.logger.exception(
                    "%s | Pipeline failure: %s",
                    dataset,
                    exc,
                )

                continue

        self.logger.info("")

        self.logger.info(
            "=" * 80
        )

        self.logger.info(
            "INGESTION PIPELINE FINISHED"
        )

        self.logger.info(
            "=" * 80
        )

    # =========================================================================
    # METRICS REPORTING
    # =========================================================================

    def report_metrics(
        self,
    ) -> None:
        """
        Report producer execution statistics.
        """

        self.logger.info("")

        self.logger.info(
            "=" * 80
        )

        self.logger.info(
            "PRODUCER EXECUTION SUMMARY"
        )

        self.logger.info(
            "=" * 80
        )

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
            "Kafka errors       : %d",
            self.metrics.kafka_errors,
        )

        self.logger.info(
            "Elapsed time       : %.2f seconds",
            self.metrics.elapsed_seconds,
        )

        self.logger.info(
            "Throughput         : %.2f records/sec",
            self.metrics.throughput,
        )

        self.logger.info(
            "=" * 80
        )


    # =========================================================================
    # SHUTDOWN
    # =========================================================================

    def shutdown(
        self,
    ) -> None:
        """
        Shut down the producer gracefully.

        This method is safe to call multiple times.
        """

        self.logger.info("")

        self.logger.info(
            "=" * 80
        )

        self.logger.info(
            "SHUTTING DOWN PRODUCER"
        )

        self.logger.info(
            "=" * 80
        )

        #
        # Flush and close Kafka producer
        #
        if self.producer is not None:

            try:

                self.logger.info(
                    "Flushing Kafka producer..."
                )

                self.producer.flush()

                self.logger.info(
                    "Kafka producer flushed."
                )

            except Exception as exc:

                self.metrics.kafka_errors += 1

                self.logger.exception(
                    "Kafka flush failed: %s",
                    exc,
                )

            try:

                self.producer.close()

                self.logger.info(
                    "Kafka producer closed."
                )

            except Exception as exc:

                self.metrics.kafka_errors += 1

                self.logger.exception(
                    "Kafka close failed: %s",
                    exc,
                )

            finally:

                self.producer = None

        #
        # Close HTTP session
        #
        if self.session is not None:

            try:

                self.session.close()

                self.logger.info(
                    "HTTP session closed."
                )

            except Exception as exc:

                self.logger.exception(
                    "HTTP session close failed: %s",
                    exc,
                )

            finally:

                self.session = None

        #
        # Final execution report
        #
        self.report_metrics()


    # =========================================================================
    # APPLICATION ENTRY
    # =========================================================================

    def run(
        self,
    ) -> int:
        """
        Execute the producer application.

        Returns
        -------
        int
            Process exit code.
        """

        exit_code = 0

        try:

            self.run_pipeline()

            self.logger.info(
                "Producer completed successfully."
            )

        except KeyboardInterrupt:

            self.logger.info(
                "Execution interrupted by user."
            )

        except Exception as exc:

            exit_code = 1

            self.logger.exception(
                "Fatal application error: %s",
                exc,
            )

        finally:

            self.shutdown()

        return exit_code

    # =========================================================================
    # END OF KafkaProducerApp
    # =========================================================================

    #
    # No additional methods belong below this point.
    #
    # The next top-level definition in this file must be:
    #
    #     class LogContext:
    #
    # with ZERO indentation.
    #



# =============================================================================
# PRODUCTION LOGGING UTILITIES
# =============================================================================

class LogContext:
    """
    Helper methods for consistent operational logging.
    """

    @staticmethod
    def dataset_banner(
        logger: logging.Logger,
        dataset: str,
    ) -> None:

        logger.info("")
        logger.info("=" * 80)
        logger.info(
            "DATASET : %s",
            dataset,
        )
        logger.info("=" * 80)

    @staticmethod
    def page(
        logger: logging.Logger,
        page: int,
        records: int,
    ) -> None:

        logger.info(
            "Page %-5d | Records : %-5d",
            page,
            records,
        )

    @staticmethod
    def cursor(
        logger: logging.Logger,
        cursor: dict[str, Any],
    ) -> None:

        logger.info(
            "Cursor : %s",
            cursor,
        )

    @staticmethod
    def completion(
        logger: logging.Logger,
        dataset: str,
        total: int,
    ) -> None:

        logger.info(
            "%s complete (%d records)",
            dataset,
            total,
        )


# =============================================================================
# EXCEPTION UTILITIES
# =============================================================================

def log_exception(
    logger: logging.Logger,
    message: str,
    exc: Exception,
) -> None:
    """
    Standardised exception logging.
    """

    logger.exception(
        "%s : %s",
        message,
        exc,
    )

    # =============================================================================
# APPLICATION ENTRY POINT
# =============================================================================

def main() -> int:
    """
    Application entry point.
    """

    app = KafkaProducerApp()

    return app.run()


# =============================================================================
# PYTHON MODULE ENTRY
# =============================================================================

if __name__ == "__main__":

    sys.checkpoint_field