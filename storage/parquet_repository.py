"""
Parquet Repository
==================

Production-grade repository for persisting raw FEMA datasets.

Responsibilities
----------------
* Load raw FEMA datasets from Parquet storage.
* Persist raw streamed records.
* Deduplicate records using the configured dataset primary key.
* Expose repository statistics.
* Isolate all storage concerns from the Kafka consumer.

This repository intentionally has no knowledge of:

    - Kafka
    - Kafka Topics
    - Kafka Messages
    - Feature Engineering
    - Machine Learning
    - Checkpoint Management

It operates exclusively on raw FEMA records.
"""

from __future__ import annotations

# ==========================================================
# Standard Library
# ==========================================================

import logging
import os
import tempfile
from pathlib import Path
from typing import Any

# ==========================================================
# Third-Party Libraries
# ==========================================================

import pandas as pd

# ==========================================================
# Project Imports
# ==========================================================

from storage.config import (
    ENDPOINTS,
    RAW_DATA_DIR,
)

# ==========================================================
# Module Logger
# ==========================================================

logger = logging.getLogger(__name__)


# ==========================================================
# Parquet Repository
# ==========================================================

class ParquetRepository:
    """
    Repository responsible for persisting raw FEMA datasets.

    The repository provides a storage abstraction for loading,
    merging and saving raw FEMA datasets irrespective of how
    the records were ingested (batch or Kafka streaming).

    All dataset-specific behaviour is derived dynamically from
    ``configuration.ENDPOINTS`` so that new datasets can be
    introduced without modifying repository logic.
    """

    def __init__(
        self,
        compression: str = "gzip",
    ) -> None:
        """
        Initialise the repository.

        Parameters
        ----------
        compression
            Compression codec used when writing Parquet files.
        """

        self.compression = compression

        RAW_DATA_DIR.mkdir(
            parents=True,
            exist_ok=True,
        )

        logger.info(
            "ParquetRepository initialised (directory=%s, compression=%s)",
            RAW_DATA_DIR,
            compression,
        )

    # ======================================================
    # Internal Helpers
    # ======================================================

    def _validate_dataset(
        self,
        dataset: str,
    ) -> None:
        """
        Ensure the supplied dataset exists in ENDPOINTS.

        Raises
        ------
        ValueError
            If the dataset is not configured.
        """

        if dataset not in ENDPOINTS:

            valid = ", ".join(sorted(ENDPOINTS.keys()))

            raise ValueError(
                f"Unknown dataset '{dataset}'. "
                f"Configured datasets: {valid}"
            )

    # ======================================================
    # Path Helpers
    # ======================================================

    def dataset_path(
        self,
        dataset: str,
    ) -> Path:
        """
        Return the Parquet file location for a dataset.

        Example
        -------
        declarations
            -> data/raw/declarations.parquet
        """

        self._validate_dataset(dataset)

        return RAW_DATA_DIR / f"{dataset}.parquet"

    # ======================================================
    # Dataset Metadata
    # ======================================================

    def dataset_metadata(
        self,
        dataset: str,
    ) -> dict[str, Any]:
        """
        Return the configuration metadata for a dataset.
        """

        self._validate_dataset(dataset)

        return ENDPOINTS[dataset]

    def key_field(
        self,
        dataset: str,
    ) -> str:
        """
        Return the configured primary key field.
        """

        return self.dataset_metadata(dataset)["key_field"]

    def checkpoint_field(
        self,
        dataset: str,
    ) -> str:
        """
        Return the configured checkpoint field.

        The repository does not currently use checkpoint
        information directly, but exposing it here keeps the
        repository aligned with the shared metadata model.
        """

        return self.dataset_metadata(dataset)["checkpoint_field"]

    # ======================================================
    # Dataset Information
    # ======================================================

    def exists(
        self,
        dataset: str,
    ) -> bool:
        """
        Determine whether a Parquet dataset already exists.
        """

        return self.dataset_path(dataset).exists()

    def row_count(
        self,
        dataset: str,
    ) -> int:
        """
        Return the number of persisted records.
        """

        if not self.exists(dataset):
            return 0

        return len(pd.read_parquet(self.dataset_path(dataset)))

    def file_size_mb(
        self,
        dataset: str,
    ) -> float:
        """
        Return the Parquet file size in megabytes.
        """

        if not self.exists(dataset):
            return 0.0

        return round(
            self.dataset_path(dataset).stat().st_size / (1024 * 1024),
            2,
        )
        # ======================================================
    # RECORD VALIDATION
    # ======================================================

    def _validate_records(
        self,
        records: list[Any],
    ) -> None:
        """
        Validate incoming records before persistence.

        Parameters
        ----------
        records
            Raw FEMA records.

        Raises
        ------
        TypeError
            If records is not a list of dictionaries.
        """

        if not isinstance(records, list):
            raise TypeError(
                "records must be a list of dictionaries."
            )

        for index, record in enumerate(records):

            if not isinstance(record, dict):

                raise TypeError(
                    f"Record {index} is not a dictionary."
                )

    # ======================================================
    # LOAD
    # ======================================================

    def load(
        self,
        dataset: str,
    ) -> pd.DataFrame:
        """
        Load a persisted dataset.

        Returns an empty DataFrame if the dataset
        does not yet exist.
        """

        path = self.dataset_path(dataset)

        if not path.exists():

            logger.info(
                "%s | No existing dataset found.",
                dataset,
            )

            return pd.DataFrame()

        try:

            dataframe = pd.read_parquet(path)

            logger.info(
                "%s | Loaded %d rows.",
                dataset,
                len(dataframe),
            )

            return dataframe

        except Exception:

            logger.exception(
                "%s | Failed to load parquet file '%s'.",
                dataset,
                path,
            )

            raise

    # ======================================================
    # SAVE
    # ======================================================

    def save(
        self,
        dataset: str,
        dataframe: pd.DataFrame,
    ) -> None:
        """
        Persist a dataframe atomically.

        Data is first written to a uniquely named temporary
        file in the destination directory before replacing
        the target parquet file using an atomic rename.
        """

        path = self.dataset_path(dataset)

        fd, tmp_name = tempfile.mkstemp(
            suffix=".parquet.tmp",
            dir=path.parent,
        )

        os.close(fd)

        temp_path = Path(tmp_name)

        try:

            dataframe.to_parquet(
                temp_path,
                index=False,
                compression=self.compression,
            )

            os.replace(
                temp_path,
                path,
            )

            logger.info(
                "%s | Persisted %d rows.",
                dataset,
                len(dataframe),
            )

        except Exception:

            if temp_path.exists():

                temp_path.unlink(
                    missing_ok=True,
                )

            logger.exception(
                "%s | Failed saving parquet dataset.",
                dataset,
            )

            raise

    # ======================================================
    # DEDUPLICATION
    # ======================================================

    def deduplicate(
        self,
        dataset: str,
        dataframe: pd.DataFrame,
    ) -> tuple[pd.DataFrame, int]:
        """
        Remove duplicate records.

        Duplicate Policy
        ----------------
        Records are deduplicated using the configured
        dataset primary key.

        If duplicate keys exist, the most recently
        received record is retained (keep='last').
        """

        if dataframe.empty:

            return dataframe, 0

        key_field = self.key_field(dataset)

        if key_field not in dataframe.columns:

            raise KeyError(
                f"{dataset}: "
                f"Configured key field '{key_field}' "
                "is missing from the dataframe."
            )

        before = len(dataframe)

        dataframe = dataframe.drop_duplicates(
            subset=key_field,
            keep="last",
        )

        removed = before - len(dataframe)

        if removed:

            logger.info(
                "%s | Removed %d duplicate records.",
                dataset,
                removed,
            )

        return dataframe, removed

    # ======================================================
    # APPEND
    # ======================================================

    def append(
        self,
        dataset: str,
        records: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """
        Append raw FEMA records.

        Parameters
        ----------
        dataset
            Dataset identifier.

        records
            Raw FEMA payloads.

        Returns
        -------
        Dictionary containing repository statistics.
        """

        self._validate_records(records)

        if not records:

            logger.info(
                "%s | No records to append.",
                dataset,
            )

            return {
                "dataset": dataset,
                "existing": self.row_count(dataset),
                "incoming": 0,
                "duplicates": 0,
                "total": self.row_count(dataset),
                "file_size_mb": self.file_size_mb(dataset),
            }

        incoming = pd.DataFrame.from_records(records)

        key_field = self.key_field(dataset)

        if key_field not in incoming.columns:

            raise ValueError(
                f"{dataset}: incoming records "
                f"do not contain required key field "
                f"'{key_field}'."
            )

        existing = self.load(dataset)

        existing_rows = len(existing)

        incoming_rows = len(incoming)

        logger.info(
            "%s | Existing=%d Incoming=%d",
            dataset,
            existing_rows,
            incoming_rows,
        )

        if existing.empty:

            merged = incoming

        else:

            merged = pd.concat(
                [
                    existing,
                    incoming,
                ],
                ignore_index=True,
                copy=False,
            )

        merged, duplicates_removed = self.deduplicate(
            dataset,
            merged,
        )

        self.save(
            dataset,
            merged,
        )

        file_size = self.file_size_mb(dataset)

        stats = {
            "dataset": dataset,
            "existing": existing_rows,
            "incoming": incoming_rows,
            "duplicates": duplicates_removed,
            "total": len(merged),
            "file_size_mb": file_size,
        }

        logger.info(
            "%s | Append complete "
            "(%d total rows, %d duplicates removed).",
            dataset,
            stats["total"],
            duplicates_removed,
        )

        return stats

    # ======================================================
    # STORAGE STATISTICS
    # ======================================================

    def statistics(
        self,
        dataset: str,
    ) -> dict[str, Any]:
        """
        Return repository statistics.
        """

        return {

            "dataset": dataset,

            "exists": self.exists(dataset),

            "rows": self.row_count(dataset),

            "file_size_mb": self.file_size_mb(dataset),

            "key_field": self.key_field(dataset),

            "checkpoint_field": self.checkpoint_field(dataset),

            "path": str(
                self.dataset_path(dataset),
            ),

        }

        # ======================================================
    # STORAGE STATISTICS
    # ======================================================

    def log_statistics(
        self,
        dataset: str,
    ) -> None:
        """
        Log repository statistics for a dataset.

        Intended for operational logging after batch
        ingestion or Kafka consumer flushes.
        """

        stats = self.statistics(dataset)

        logger.info(
            "========== Repository Statistics =========="
        )

        logger.info(
            "Dataset           : %s",
            stats["dataset"],
        )

        logger.info(
            "Exists            : %s",
            stats["exists"],
        )

        logger.info(
            "Rows              : %d",
            stats["rows"],
        )

        logger.info(
            "Primary Key       : %s",
            stats["key_field"],
        )

        logger.info(
            "Checkpoint Field  : %s",
            stats["checkpoint_field"],
        )

        logger.info(
            "File Size (MB)    : %.2f",
            stats["file_size_mb"],
        )

        logger.info(
            "Location          : %s",
            stats["path"],
        )

        logger.info(
            "==========================================="
        )

    # ======================================================
    # DATASET MAINTENANCE
    # ======================================================

    def clear(
        self,
        dataset: str,
    ) -> None:
        """
        Delete a persisted dataset.

        Primarily intended for integration testing and
        development environments.
        """

        path = self.dataset_path(dataset)

        if not path.exists():

            logger.info(
                "%s | Dataset does not exist.",
                dataset,
            )

            return

        try:

            path.unlink()

            logger.warning(
                "%s | Deleted dataset '%s'.",
                dataset,
                path,
            )

        except Exception:

            logger.exception(
                "%s | Failed deleting dataset.",
                dataset,
            )

            raise

    # ======================================================
    # CONFIGURED DATASETS
    # ======================================================

    def datasets(
        self,
    ) -> list[str]:
        """
        Return configured FEMA dataset names.

        Returns
        -------
        list[str]
            Sorted dataset identifiers defined in
            configuration.ENDPOINTS.
        """

        return sorted(
            ENDPOINTS.keys()
        )

    # ======================================================
    # STRING REPRESENTATION
    # ======================================================

    def __repr__(
        self,
    ) -> str:
        """
        Developer-friendly repository representation.
        """

        return (
            f"{self.__class__.__name__}("
            f"compression='{self.compression}', "
            f"raw_directory='{RAW_DATA_DIR}')"
        )