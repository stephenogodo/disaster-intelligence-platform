"""
Platform Runner

Coordinates execution of registered platform services.
"""

from __future__ import annotations

import logging

from orchestration.services.base_service import BaseService

logger = logging.getLogger(__name__)


class PlatformRunner:
    """
    Executes registered platform services.
    """

    def __init__(self) -> None:
        self.services: list[BaseService] = []

        logger.info(
            "PlatformRunner initialised."
        )

    def register_service(
        self,
        service: BaseService,
    ) -> None:
        """
        Register a platform service.
        """
        self.services.append(service)

        logger.info(
            "Registered service: %s",
            service.name,
        )

    def run(self) -> int:
        """
        Execute all registered services.
        """
        logger.info("=" * 70)
        logger.info("PLATFORM EXECUTION STARTED")
        logger.info("=" * 70)

        try:
            for service in self.services:

                logger.info(
                    "Starting %s...",
                    service.name,
                )

                service.start()

                if service.service_type == "batch":
                    logger.info(
                        "%s completed successfully.",
                        service.name,
                    )
                else:
                    logger.info(
                        "%s is running.",
                        service.name,
                    )

            logger.info(
                "Platform execution completed successfully."
            )

            return 0

        except Exception:
            logger.exception(
                "Platform execution failed."
            )
            return 1

        finally:
            logger.info("=" * 70)
            logger.info("PLATFORM EXECUTION FINISHED")
            logger.info("=" * 70)
