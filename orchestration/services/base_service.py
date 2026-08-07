"""
Base Service

Defines the common interface implemented by every platform service.
"""

from __future__ import annotations

from abc import ABC, abstractmethod


class BaseService(ABC):
    """
    Abstract base class for all platform services.
    """
@property
@abstractmethod
def service_type(self) -> str:
    """
    Return the service type.

    Valid values:
        "batch"
        "stream"
    """
    pass

    @property
    @abstractmethod
    def name(self) -> str:
        """
        Human-readable service name.
        """
        pass

    @abstractmethod
    def start(self) -> None:
        """
        Start the service.
        """
        pass

    @abstractmethod
    def stop(self) -> None:
        """
        Stop the service gracefully.
        """
        pass

    @abstractmethod
    def status(self) -> str:
        """
        Return the current service status.
        """
        pass