"""
Kafka Consumer

Consumes raw FEMA events from Kafka and persists them as raw
Parquet datasets via ParquetRepository.

Responsibilities
----------------
* Consume Kafka events.
* Validate event envelopes.
* Buffer payloads by dataset.
* Flush buffered payloads to ParquetRepository.
* Commit Kafka offsets only after successful persistence.
* Support graceful shutdown.

This consumer intentionally does NOT:

    * Perform feature engineering.
    * Modify FEMA records.
    * Deduplicate records.
    * Manage checkpoints.
    * Perform machine learning.

Those responsibilities belong elsewhere.
"""

from __future__ import annotations

# ==========================================================
# Standard Library
# ==========================================================

import json
import logging
import signal
from collections import defaultdict
from typing import Any

# ==========================================================
# Third-Party Libraries
# ==========================================================

from kafka import KafkaConsumer

# ==========================================================
# Project Imports
# ==========================================================
from storage.config import (
    ENDPOINTS,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_GROUP_ID,
    KAFKA_POLL_TIMEOUT_MS,
    KAFKA_MAX_BATCH_SIZE,
)

from storage.parquet_repository import (
    ParquetRepository,
)
# ==========================================================
# Module Logger
# ==========================================================

logger = logging.getLogger(__name__)


# ==========================================================
# Kafka Consumer Service
# ==========================================================

class FEMAKafkaConsumer:
    """
    Production Kafka consumer for FEMA streaming ingestion.

    One instance owns:

        • Kafka consumer
        • In-memory dataset buffers
        • Repository
        • Graceful shutdown lifecycle
    """

    def __init__(self) -> None:

        self.running = True

        self.repository = ParquetRepository()

        #
        # Buffer records independently per dataset.
        #
        # Example
        #
        # {
        #   "declarations": [...],
        #   "public_assistance": [...],
        # }
        #
        self.buffers: dict[str, list[dict[str, Any]]] = defaultdict(list)

        self.consumer = self._create_consumer()

        self._register_signal_handlers()

        logger.info(
            "Kafka consumer initialised."
        )

    # ======================================================
    # Kafka Consumer Factory
    # ======================================================

    def _create_consumer(
        self,
    ) -> KafkaConsumer:
        """
        Create the Kafka consumer.

        Manual offset commits are used to guarantee that
        offsets are only committed after successful
        persistence.
        """

        logger.info(
            "Connecting to Kafka brokers: %s",
            KAFKA_BOOTSTRAP_SERVERS,
        )

        consumer = KafkaConsumer(

            KAFKA_TOPIC,

            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,

            group_id=KAFKA_GROUP_ID,

            enable_auto_commit=False,

            auto_offset_reset="earliest",

            value_deserializer=lambda value: json.loads(
                value.decode("utf-8")
            ),

            consumer_timeout_ms=1000,
        )

        logger.info(
            "Connected to topic '%s'.",
            KAFKA_TOPIC,
        )

        return consumer

    # ======================================================
    # Signal Handling
    # ======================================================

    def _register_signal_handlers(
        self,
    ) -> None:
        """
        Register shutdown handlers.
        """

        signal.signal(
            signal.SIGINT,
            self._shutdown_signal,
        )

        signal.signal(
            signal.SIGTERM,
            self._shutdown_signal,
        )

    def _shutdown_signal(
        self,
        signum: int,
        frame: Any,
    ) -> None:
        """
        Handle termination signals.
        """

        logger.warning(
            "Shutdown signal received (%s).",
            signum,
        )

        self.running = False

    # ======================================================
    # Utility Methods
    # ======================================================

    @staticmethod
    def _is_valid_dataset(
        dataset: str,
    ) -> bool:
        """
        Determine whether a dataset is configured.
        """

        return dataset in ENDPOINTS

    @staticmethod
    def _buffer_size(
        buffer: list[dict[str, Any]],
    ) -> int:
        """
        Return the current buffer size.
        """

        return len(buffer)

    def _should_flush(
        self,
        dataset: str,
    ) -> bool:
        """
        Determine whether a dataset buffer should be flushed.
        """

        return (
            self._buffer_size(
                self.buffers[dataset]
            )
            >= KAFKA_MAX_BATCH_SIZE
        )
        # ======================================================
    # EVENT VALIDATION
    # ======================================================

    def _validate_event(
        self,
        event: dict[str, Any],
    ) -> tuple[str, dict[str, Any]] | None:
        """
        Validate a Kafka event.

        Expected envelope:

        {
            "schema_version": "...",
            "dataset": "...",
            "event_time": "...",
            "record_id": "...",
            "payload": {...}
        }

        Returns
        -------
        (dataset, payload)

        Returns None if validation fails.
        """

        if not isinstance(event, dict):

            logger.warning(
                "Discarding event: expected dictionary."
            )

            return None

        dataset = event.get("dataset")

        if not dataset:

            logger.warning(
                "Discarding event: missing dataset."
            )

            return None

        if not self._is_valid_dataset(dataset):

            logger.warning(
                "Discarding event: unknown dataset '%s'.",
                dataset,
            )

            return None

        payload = event.get("payload")

        if not isinstance(payload, dict):

            logger.warning(
                "Discarding event: invalid payload for '%s'.",
                dataset,
            )

            return None

        return dataset, payload

    # ======================================================
    # BUFFER MANAGEMENT
    # ======================================================

    def _buffer_record(
        self,
        dataset: str,
        payload: dict[str, Any],
    ) -> None:
        """
        Add a validated payload to the dataset buffer.
        """

        self.buffers[dataset].append(payload)

        logger.debug(
            "%s | Buffered record (%d buffered).",
            dataset,
            len(self.buffers[dataset]),
        )

    # ======================================================
    # FLUSH
    # ======================================================

    def _flush_dataset(
        self,
        dataset: str,
    ) -> bool:
        """
        Persist a buffered dataset.

        Returns
        -------
        bool
            True if persistence succeeded.
            False if persistence failed.
        """

        records = self.buffers[dataset]

        if not records:

            return True

        logger.info(
            "%s | Flushing %d records.",
            dataset,
            len(records),
        )

        try:

            stats = self.repository.append(
                dataset=dataset,
                records=records,
            )

            logger.info(
                "%s | Persisted %d records "
                "(duplicates=%d total=%d).",
                dataset,
                stats["incoming"],
                stats["duplicates"],
                stats["total"],
            )

            #
            # Only clear the buffer after a successful write.
            #
            self.buffers[dataset].clear()

            return True

        except Exception:

            logger.exception(
                "%s | Repository write failed.",
                dataset,
            )

            #
            # Leave the buffer intact so the records
            # are retried on the next flush.
            #
            return False

    def _flush_all(self) -> bool:
        """
        Flush every dataset buffer.

        Returns
        -------
        bool
            True only if every dataset was
            persisted successfully.
        """

        success = True

        for dataset in sorted(self.buffers.keys()):

            if not self._flush_dataset(dataset):

                success = False

        return success

    # ======================================================
    # OFFSET MANAGEMENT
    # ======================================================

    def _commit_offsets(self) -> None:
        """
        Commit Kafka offsets.

        Offsets are committed only after
        successful persistence.
        """

        try:

            self.consumer.commit()

            logger.info(
                "Kafka offsets committed."
            )

        except Exception:

            logger.exception(
                "Kafka offset commit failed."
            )

            raise

    # ======================================================
    # MESSAGE PROCESSING
    # ======================================================

    def _process_message(
        self,
        event: dict[str, Any],
    ) -> None:
        """
        Process one Kafka event.
        """

        validated = self._validate_event(event)

        if validated is None:

            return

        dataset, payload = validated

        self._buffer_record(
            dataset,
            payload,
        )

        if not self._should_flush(dataset):

            return

        if self._flush_dataset(dataset):

            self._commit_offsets()


    # ======================================================
    # MAIN CONSUMER LOOP
    # ======================================================

    def run(self) -> None:
        """
        Start the Kafka consumer.

        Messages are processed continuously until a shutdown
        signal is received.
        """

        logger.info("Kafka consumer started.")

        try:

            while self.running:

                records = self.consumer.poll(
                    timeout_ms=KAFKA_POLL_TIMEOUT_MS,
                )

                if not records:
                    continue

                for _, messages in records.items():

                    for message in messages:

                        try:

                            self._process_message(
                                message.value,
                            )

                        except Exception:

                            logger.exception(
                                "Unexpected error processing Kafka message."
                            )

        except KeyboardInterrupt:

            logger.info(
                "Keyboard interrupt received."
            )

        finally:

            self.shutdown()

    # ======================================================
    # SHUTDOWN
    # ======================================================

    def shutdown(self) -> None:
        """
        Gracefully stop the consumer.

        Shutdown sequence

            Stop consuming
                    ↓
            Flush buffers
                    ↓
            Commit offsets
                    ↓
            Close consumer
        """

        logger.info(
            "Shutting down Kafka consumer..."
        )

        try:

            #
            # Flush remaining buffered records.
            #
            success = self._flush_all()

            #
            # Commit offsets only if every dataset
            # was persisted successfully.
            #
            if success:

                self._commit_offsets()

            else:

                logger.warning(
                    "One or more dataset flushes failed. "
                    "Kafka offsets were NOT committed."
                )

        finally:

            try:

                self.consumer.close()

                logger.info(
                    "Kafka consumer closed."
                )

            except Exception:

                logger.exception(
                    "Failed closing Kafka consumer."
                )

        logger.info(
            "Kafka consumer stopped."
        )


# ==========================================================
# MAIN ENTRY POINT
# ==========================================================

def main() -> None:
    """
    Application entry point.
    """

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | "
            "%(levelname)s | "
            "%(name)s | "
            "%(message)s"
        ),
    )

    consumer = FEMAKafkaConsumer()

    consumer.run()


if __name__ == "__main__":

    main()