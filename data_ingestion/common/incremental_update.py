"""
Incremental Parquet Update Utility

Shared by
---------
• Batch FEMA ingestion
• Kafka streaming ingestion
• Future Airflow DAGs

Responsibilities
----------------
1. Stage incoming data
2. Load existing datasets
3. Merge staged data
4. Remove duplicates
5. Persist datasets atomically
6. Clean staging area
"""

from pathlib import Path
import logging
import uuid

import pandas as pd

logger = logging.getLogger(__name__)


# ============================================================
# PROJECT PATHS
# ============================================================
from storage.config import (
    RAW_DATA_DIR,
    STAGING_DIR,
)
# ============================================================
# DATASET DEDUPLICATION KEYS
# ============================================================

DEDUP_KEYS = {

    "declarations": [
        "id",
    ],

    "public_assistance": [
        "gmProjectId",
    ],

    "disaster_summaries": [
        "id",
    ],

}

RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
STAGING_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================
# STAGING
# ============================================================


def create_staging_file(dataset_name: str) -> Path:
    """
    Create a unique staging parquet file.
    """

    dataset_stage = STAGING_DIR / dataset_name

    if not dataset_stage.exists():
        logger.info(
            f"Creating staging directory for {dataset_name}"
        )

        dataset_stage.mkdir(
            parents=True,
            exist_ok=True,
        )

    filename = f"chunk_{uuid.uuid4().hex}.parquet"

    return dataset_stage / filename

def stage_chunk(
    df: pd.DataFrame,
    dataset_name: str,
) -> Path:
    """
    Persist one downloaded page into
    the staging area.
    """

    staging_file = create_staging_file(
        dataset_name
    )

    df.to_parquet(
        staging_file,
        index=False,
    )

    logger.info(
        f"Staged {len(df):,} rows "
        f"→ {staging_file.name}"
    )

    return staging_file


# ============================================================
# EXISTING DATA
# ============================================================


def load_existing(
    file_path: Path,
) -> pd.DataFrame:
    """
    Load an existing dataset.

    Returns an empty dataframe if the
    dataset does not yet exist.
    """

    if file_path.exists():

        logger.info(
            f"Loading existing dataset "
            f"{file_path.name}"
        )

        return pd.read_parquet(file_path)

    logger.info(
        f"{file_path.name} "
        f"does not exist."
    )

    return pd.DataFrame()


# ============================================================
# MERGING
# ============================================================


def merge_data(
    existing_df: pd.DataFrame,
    new_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Merge two datasets.
    """

    if existing_df.empty:
        return new_df.copy()

    if new_df.empty:
        return existing_df.copy()

    logger.info(
        f"Merging "
        f"{len(existing_df):,} "
        f"existing rows with "
        f"{len(new_df):,} "
        f"new rows."
    )

    frames = [
    df
    for df in (existing_df, new_df)
    if not df.empty
]

    if not frames:
        return pd.DataFrame()

    return pd.concat(
    frames,
    ignore_index=True,
)


# ============================================================
# DEDUPLICATION
# ============================================================

def deduplicate(
    df: pd.DataFrame,
    subset=None,
):
    """
    Remove duplicate rows while preserving
    the most recent record based on lastRefresh.

    Returns
    -------
    dataframe,
    duplicates_removed
    """

    before = len(df)

    # Keep the latest version of each record
    if "lastRefresh" in df.columns:
        df = df.sort_values("lastRefresh")

    df = df.drop_duplicates(
        subset=subset,
        keep="last",
    )

    removed = before - len(df)

    logger.info(
        f"Removed {removed:,} duplicate rows."
    )

    return df, removed


def save_atomic(
    df: pd.DataFrame,
    file_path: Path,
):
    """
    Atomically save a DataFrame to a parquet file.

    The dataset is first written to a temporary file and
    then moved into place to avoid partial writes.
    """

    file_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp_file = file_path.with_suffix(".tmp.parquet")

    df.to_parquet(
        temp_file,
        index=False,
    )

    temp_file.replace(file_path)

    logger.info(
        f"Saved dataset → {file_path.name}"
    )

# ============================================================
# STAGING CLEANUP
# ============================================================

def cleanup_staging(
    dataset_name: str,
) -> int:
    """
    Remove all staged parquet chunks belonging
    to one dataset.
    """

    dataset_stage = STAGING_DIR / dataset_name

    if not dataset_stage.exists():
        return 0

    removed = 0

    for file in dataset_stage.glob("*.parquet"):
        file.unlink()
        removed += 1

    logger.info(
        f"Removed {removed} staging file(s)."
    )

    return removed


# ============================================================
# FINAL INGESTION
# ============================================================

def update_parquet(
    dataset_name: str,
):
    """
    Merge staged parquet chunks into the
    production dataset.

    Workflow
    --------
    1. Load existing parquet
    2. Read staged chunks
    3. Merge incrementally
    4. Deduplicate
    5. Save atomically
    6. Cleanup staging
    """

    dataset_stage = STAGING_DIR / dataset_name

    chunk_files = sorted(
        dataset_stage.glob("*.parquet")
    )

    if not chunk_files:

        logger.info(
            f"No staged chunks found for "
            f"{dataset_name}"
        )

        return None

    file_path = RAW_DATA_DIR / f"{dataset_name}.parquet"

    existing = load_existing(file_path)

    existing_rows = len(existing)

    new_rows = 0

    logger.info(
    f"Processing {len(chunk_files)} staged chunk(s) "
    f"from {dataset_stage}"
)

    for chunk in chunk_files:

        df = pd.read_parquet(chunk)

        new_rows += len(df)

        existing = merge_data(
            existing,
            df,
        )

        del df

    subset = DEDUP_KEYS.get(
    dataset_name,
)

    existing, duplicates_removed = deduplicate(
    existing,
    subset=subset,
)

    save_atomic(
    existing,
    file_path,
)

    cleanup_staging(
        dataset_name,
    )

    logger.info("=" * 60)
    logger.info(f"Dataset              : {dataset_name}")
    logger.info(f"Existing records     : {existing_rows:,}")
    logger.info(f"New records          : {new_rows:,}")
    logger.info(f"Duplicates removed   : {duplicates_removed:,}")
    logger.info(f"Final records        : {len(existing):,}")
    logger.info("=" * 60)

    return {
        "dataset": dataset_name,
        "existing_rows": existing_rows,
        "new_rows": new_rows,
        "duplicates_removed": duplicates_removed,
        "final_rows": len(existing),
    }