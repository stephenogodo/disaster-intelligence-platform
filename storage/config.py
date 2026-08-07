"""
Central configuration for the Disaster Intelligence Platform.

This module contains project-wide configuration only.
No business logic belongs here.
"""

from pathlib import Path

# =============================================================================
# PROJECT PATHS
# =============================================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
STAGING_DIR = DATA_DIR / "staging"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

METADATA_DIR = PROJECT_ROOT / "metadata"
LOG_DIR = PROJECT_ROOT / "logs"
MODELS_DIR = PROJECT_ROOT / "models"
REPORTS_DIR = PROJECT_ROOT / "reports"

CHECKPOINT_FILE = METADATA_DIR / "checkpoints.json"

for directory in (
    DATA_DIR,
    RAW_DATA_DIR,
    PROCESSED_DATA_DIR,
    STAGING_DIR,
    METADATA_DIR,
    LOG_DIR,
    MODELS_DIR,
    REPORTS_DIR,
):
    directory.mkdir(parents=True, exist_ok=True)

# =============================================================================
# FEMA API
# =============================================================================

BASE_URL = "https://www.fema.gov/api/open"

# =============================================================================
# KAFKA CONFIGURATION
# =============================================================================

KAFKA_BOOTSTRAP = "localhost:9092"

# Consumer compatibility
KAFKA_BOOTSTRAP_SERVERS = KAFKA_BOOTSTRAP

KAFKA_TOPIC = "fema_raw"

KAFKA_GROUP_ID = "fema-stream"

KAFKA_POLL_TIMEOUT_MS = 1000

KAFKA_MAX_BATCH_SIZE = 1000

KAFKA_SEND_TIMEOUT = 60

# =============================================================================
# HTTP CONFIGURATION
# =============================================================================

REQUEST_TIMEOUT = 60

MAX_RETRIES = 5

BACKOFF_FACTOR = 2

HTTP_RETRY_DELAY = 5

SLEEP_BETWEEN_REQUESTS = 1

PAGE_SIZE = 1000

SCHEMA_VERSION = "3.0"

# =============================================================================
# FEMA DATASETS
# =============================================================================

ENDPOINTS = {

    "declarations": {

        "url": f"{BASE_URL}/v2/DisasterDeclarationsSummaries",

        "key_field": "disasterNumber",

        "checkpoint_field": "lastRefresh",

        "select": [
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

    "url": f"{BASE_URL}/v2/PublicAssistanceFundedProjectsDetails",

    # Unique key
    "key_field": "gmProjectId",

    # Incremental checkpoint
    "checkpoint_field": "lastRefresh",

    "select": [

        "disasterNumber",
        "declarationDate",
        "incidentType",
        "pwNumber",
        "applicationTitle",
        "applicantId",
        "damageCategoryCode",
        "damageCategoryDescrip",
        "projectStatus",
        "projectProcessStep",
        "projectSize",
        "county",
        "countyCode",
        "stateAbbreviation",
        "stateNumberCode",
        "projectAmount",
        "federalShareObligated",
        "totalObligated",
        "lastObligationDate",
        "firstObligationDate",
        "mitigationAmount",
        "gmProjectId",
        "gmApplicantId",
        "lastRefresh",
        "hash",

    ],
},
        "disaster_summaries": {

        "url": f"{BASE_URL}/v1/FemaWebDisasterSummaries",

        "key_field": "disasterNumber",

        "checkpoint_field": "lastRefresh",

        "select": [

        "disasterNumber",

        "totalNumberIaApproved",

        "totalAmountIhpApproved",

        "totalAmountHaApproved",

        "totalAmountOnaApproved",

        "totalObligatedAmountPa",

        "totalObligatedAmountCatAb",

        "totalObligatedAmountCatC2g",

        "totalObligatedAmountHmgp",

        "paLoadDate",

        "iaLoadDate",

        "hash",

        "lastRefresh",

        "id",

    ],
},
}