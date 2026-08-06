"""
FEMA Incremental Data Ingestion

Architecture
------------
1. Read checkpoint
2. Resume download if interrupted
3. Query FEMA incrementally using lastRefresh
4. Download in pages
5. Stage every page
6. Merge staged chunks
7. Update checkpoint
"""

import logging
import time

import pandas as pd
import requests

from data_ingestion.common.incremental_update import (
    cleanup_staging,
    stage_chunk,
    update_parquet,
)

from storage.checkpoint_manager import (
    get_checkpoint,
    update_checkpoint,
)

from storage.download_state import (
    load_state,
    save_state,
    delete_state,
)

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)

# ============================================================
# CONFIGURATION
# ============================================================

PAGE_SIZE = 1000
REQUEST_TIMEOUT = 60
MAX_RETRIES = 8

session = requests.Session()

# ============================================================
# FEMA ENDPOINTS
# ============================================================

ENDPOINTS = {

    "declarations": {

        "url":
            "https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries",

        "select": [
            "id",
            "hash",
            "disasterNumber",
            "state",
            "incidentType",
            "declarationDate",
            "incidentBeginDate",
            "incidentEndDate",
            "declarationType",
            "lastRefresh",
        ],
    },

    "public_assistance": {

        "url":
            "https://www.fema.gov/api/open/v2/PublicAssistanceFundedProjectsDetails",

        "select": [
            "gmProjectId",
            "disasterNumber",
            "projectAmount",
            "damageCategoryCode",
            "damageCategoryDescrip",
            "federalShareObligated",
            "stateAbbreviation",
            "lastRefresh",
        ],
    },

    "disaster_summaries": {

        "url":
            "https://www.fema.gov/api/open/v1/FemaWebDisasterSummaries",

        "select": None,
    },
}


# ============================================================
# HELPERS
# ============================================================

def extract_records(payload: dict):
    """
    FEMA responses wrap the dataset inside a metadata object.

    Return the first list found in the payload.
    """

    for value in payload.values():
        if isinstance(value, list):
            return value

    return []


# ============================================================
# DOWNLOAD ONE DATASET
# ============================================================

def stream_endpoint(
    dataset_name: str,
    endpoint: dict,
):

    logger.info("=" * 60)
    logger.info(f"Dataset : {dataset_name}")
    logger.info("=" * 60)

    url = endpoint["url"]
    fields = endpoint["select"]

    checkpoint = get_checkpoint(dataset_name)
    latest_refresh = checkpoint

    # --------------------------------------------------------
    # Resume state
    # --------------------------------------------------------

    state = load_state(dataset_name)

    if state.skip > 0:
        logger.info(
            f"Resuming download from $skip={state.skip:,}"
        )

    params = {
        "$top": PAGE_SIZE,
        "$skip": state.skip,
    }

    # --------------------------------------------------------
    # Incremental checkpoint
    # --------------------------------------------------------

    if checkpoint:

        logger.info(
            f"Checkpoint : {checkpoint}"
        )

        params["$filter"] = (
            f"lastRefresh gt '{checkpoint}'"
        )

        params["$orderby"] = (
            "lastRefresh asc"
        )

    else:

        logger.info(
            "No checkpoint found."
        )

        logger.info(
            "Running initial full ingestion."
        )

        cleanup_staging(dataset_name)

        logger.info(
            f"Cleared stale staging files for "
            f"{dataset_name}"
        )

    if fields:
        params["$select"] = ",".join(fields)

    total_downloaded = 0

    # ========================================================
    # Download Loop
    # ========================================================

    while True:

        success = False

        for attempt in range(MAX_RETRIES):

            try:

                response = session.get(
                    url,
                    params=params,
                    timeout=REQUEST_TIMEOUT,
                )

                response.raise_for_status()

                success = True
                break

            except requests.RequestException as ex:

                logger.warning(
                    f"{dataset_name} "
                    f"(attempt {attempt + 1}) "
                    f"{ex}"
                )

                delay = 2 ** attempt

                logger.warning(
                    f"Retrying in {delay} seconds..."
                )

                time.sleep(delay)

        if not success:

            state.status = "partial"
            save_state(state)

            raise RuntimeError(
                f"Unable to download {dataset_name}"
            )

        records = extract_records(
            response.json()
        )

        if not records:

            logger.info(
                "No additional records found."
            )

            break

        df = pd.DataFrame(records)

        if df.empty:
            break

        if "lastRefresh" in df.columns:

            latest_refresh = (
                df["lastRefresh"].max()
            )

        stage_chunk(
            df=df,
            dataset_name=dataset_name,
        )

        total_downloaded += len(df)

        # ----------------------------------------------------
        # Persist resume state
        # ----------------------------------------------------

        state.skip = params["$skip"] + PAGE_SIZE
        state.rows_downloaded = total_downloaded
        state.status = "downloading"

        save_state(state)

        del df

        params["$skip"] += PAGE_SIZE

    logger.info(
        f"Finished downloading "
        f"{total_downloaded:,} records."
    )

    # ========================================================
    # Merge staged data
    # ========================================================

    stats = update_parquet(
        dataset_name=dataset_name,
    )

    # ========================================================
    # Commit checkpoint
    # ========================================================

    if (
        stats is not None
        and latest_refresh is not None
    ):

        update_checkpoint(
            dataset_name,
            latest_refresh,
        )

        logger.info(
            f"Checkpoint updated → "
            f"{latest_refresh}"
        )

    # ========================================================
    # Download completed successfully
    # ========================================================

    delete_state(dataset_name)

    logger.info(
        f"{dataset_name} ingestion complete."
    )

    # ============================================================
# RUN ALL DATASETS
# ============================================================

def run_stream():
    """
    Execute incremental ingestion for all configured FEMA datasets.
    """

    logger.info("=" * 70)
    logger.info("FEMA Incremental Ingestion Started")
    logger.info("=" * 70)

    completed = 0
    failed = 0

    for dataset_name, endpoint in ENDPOINTS.items():

        try:

            stream_endpoint(
                dataset_name=dataset_name,
                endpoint=endpoint,
            )

            completed += 1

        except Exception as ex:

            failed += 1

            logger.exception(
                f"{dataset_name} failed: {ex}"
            )

    logger.info("=" * 70)
    logger.info("INGESTION SUMMARY")
    logger.info("=" * 70)
    logger.info(
        f"Successful datasets : {completed}"
    )
    logger.info(
        f"Failed datasets     : {failed}"
    )
    logger.info("=" * 70)


# ============================================================
# MAIN
# ============================================================

def main():
    """
    Application entry point.
    """

    run_stream()


if __name__ == "__main__":
    main()
    