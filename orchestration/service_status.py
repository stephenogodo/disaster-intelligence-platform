"""
Service Status Enumeration

Defines the lifecycle states used by all platform services.
"""

from enum import Enum


class ServiceStatus(Enum):
    """
    Standard lifecycle states for platform services.
    """

    STOPPED = "STOPPED"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    FAILED = "FAILED"