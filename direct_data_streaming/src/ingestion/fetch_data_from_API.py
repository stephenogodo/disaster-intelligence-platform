"""
FEMA Incremental Data Ingestion

Version 1 Architecture
----------------------
1. Read checkpoint
2. Query FEMA incrementally using lastRefresh
3. Download in pages
4. Stage every page
5. Finalize dataset
6. Update checkpoint
"""

from pathlib import Path
import logging
import requests
import pandas as pd

from direct_data_streaming.src.common.incremental_update import (
    stage_chunk,
    update_parquet,
)

from direct_data_streaming.src.common.checkpoint_manager import (
    get_checkpoint,
    update_checkpoint,
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

MAX_RETRIES = 3

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
]

        

    },

    "public_assistance": {
    "url": "https://www.fema.gov/api/open/v2/PublicAssistanceFundedProjectsDetails",
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
    FEMA datasets are wrapped inside
    metadata objects.

    Return only the dataset.
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

    params = {

        "$top": PAGE_SIZE,

        "$skip": 0,

    }

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

    if fields:

        params["$select"] = ",".join(fields)

    total_downloaded = 0

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

                    f"(attempt {attempt+1}) "

                    f"{ex}"

                )

        if not success:

            raise RuntimeError(

                f"Unable to download "

                f"{dataset_name}"

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

        #logger.info(

          #  f"Downloaded "

           # f"{total_downloaded:,} "

            #f"records")

        del df

        params["$skip"] += PAGE_SIZE

    logger.info(
    f"Finished downloading " f"{total_downloaded:,} records."
    )

    # =====================================================
    # Merge staged chunks into production dataset
    # =====================================================

    stats = update_parquet(
    dataset_name=dataset_name,
)

    # =====================================================
    # Update checkpoint ONLY after successful merge
    # =====================================================

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

    logger.info(
        f"{dataset_name} ingestion complete."
    )


# ============================================================
# RUN ALL DATASETS
# ============================================================

def run_stream():
    """
    Execute incremental ingestion for all
    configured FEMA datasets.
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
    logger.info(f"Successful datasets : {completed}")
    logger.info(f"Failed datasets     : {failed}")
    logger.info("=" * 70)


# ============================================================
# MAIN
# ============================================================

def main():
    """
    Entry point.
    """

    run_stream()


if __name__ == "__main__":
    main()