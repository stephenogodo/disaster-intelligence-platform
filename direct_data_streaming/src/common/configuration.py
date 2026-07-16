"""
Shared configuration for all ingestion pipelines.

Used by:
    • Batch Incremental Ingestion
    • Kafka Producer
    • Kafka Consumer
"""

from pathlib import Path

# =====================================================
# PROJECT PATHS
# =====================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = PROJECT_ROOT / "data"

RAW_DATA_DIR = DATA_DIR / "raw"

PROCESSED_DATA_DIR = DATA_DIR / "processed"

STAGING_DIR = DATA_DIR / "staging"

METADATA_DIR = DATA_DIR / "metadata"

CHECKPOINT_FILE = METADATA_DIR / "checkpoint.json"

# Ensure directories exist
for directory in (
    RAW_DATA_DIR,
    PROCESSED_DATA_DIR,
    STAGING_DIR,
    METADATA_DIR,
):
    directory.mkdir(parents=True, exist_ok=True)

# =====================================================
# API SETTINGS
# =====================================================

BASE_URL = "https://www.fema.gov/api/open"

PAGE_SIZE = 1000

REQUEST_TIMEOUT = 30

SLEEP_BETWEEN_REQUESTS = 1

MAX_RETRIES = 5

BACKOFF_FACTOR = 2

# =====================================================
# KAFKA SETTINGS
# =====================================================

KAFKA_BOOTSTRAP = "localhost:9092"

KAFKA_TOPIC = "fema_raw"

KAFKA_GROUP = "fema-stream"

# =====================================================
# DATASETS
# =====================================================

ENDPOINTS = {

    "declarations": {
        "url": f"{BASE_URL}/v2/DisasterDeclarationsSummaries",
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
        "url": f"{BASE_URL}/v2/PublicAssistanceFundedProjectsDetails",
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
        "url": f"{BASE_URL}/v1/FemaWebDisasterSummaries",
        "select": [
            "id",
            "hash",
            "disasterNumber",
            "lastRefresh",
        ],
    },
}