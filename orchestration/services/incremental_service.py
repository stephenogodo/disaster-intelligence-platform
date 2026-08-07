"""
Incremental FEMA Ingestion Service

Wraps the existing incremental ingestion pipeline and exposes it
through the common BaseService interface.
"""

from __future__ import annotations

import logging

from orchestration.services.base_service import BaseService

from data_ingestion.batch.batch_data_ingestor import (
    run_stream,
)

logger = logging.getLogger(__name__)


class IncrementalService(BaseService):
    """
    Service wrapper around the FEMA incremental ingestion pipeline.
    """
    @property
    def service_type(self) -> str:
        return "batch"
    
    def __init__(self) -> None:

        self._status = "STOPPED"

    @property
    def name(self) -> str:

        return "Incremental Ingestion"

    def start(self) -> None:

                            
        self._status = "RUNNING"

        try:

            run_stream()

            self._status = "STOPPED"

            logger.info(
                "%s completed successfully.",
                self.name,
            )

        except Exception:

            self._status = "FAILED"

            logger.exception(
                "%s failed.",
                self.name,
            )

            raise

    def stop(self) -> None:

        logger.info(
            "Stopping %s...",
            self.name,
        )

        self._status = "STOPPED"

    def status(self) -> str:

        return self._status