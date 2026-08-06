"""
Shared data source resolver.

All platform components use this module to locate
the project's raw and processed datasets.
"""

from storage.config import (
    RAW_DATA_DIR,
    PROCESSED_DATA_DIR,
)


def get_raw_directory(source: str = "batch"):
    """
    Return the raw data directory.

    The 'source' argument is retained only for
    backward compatibility.
    """
    return RAW_DATA_DIR


def get_processed_directory():
    """
    Return the processed data directory.
    """
    return PROCESSED_DATA_DIR