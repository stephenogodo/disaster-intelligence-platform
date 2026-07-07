from pdb import main
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import logging
from pathlib import Path
import os
import gc

logging.basicConfig(level=logging.INFO)
DATA_DIR = Path("data/raw")
DATA_DIR.mkdir(parents=True, exist_ok=True)


# =====================================================
# CONFIG
# =====================================================

BASE_URL = "https://www.fema.gov/api/open"

ENDPOINTS = {
    "declarations": {
        "url": "https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries",
        "select": [
            "disasterNumber",
            "state",
            "incidentType",
            "declarationDate"
        ],
    },

    "public_assistance": {
        "url": "https://www.fema.gov/api/open/v2/PublicAssistanceFundedProjectsDetails",
        "select": [
            "DisasterNumber",
            "ProjectID",
            "ProjectCategory",
            "FederalShareObligated",
            "State"
        ],
    },

    "disaster_summaries": {
        "url": "https://www.fema.gov/api/open/v1/FemaWebDisasterSummaries",
        "select": [
            "disasterNumber",
            "state",
            "incidentType"
        ],
    },
}

FIELDS = {
    "declarations": [
        "disasterNumber",
        "state",
        "incidentType",
        "declarationDate",
        "incidentBeginDate",
        "incidentEndDate",
        "declarationType",
    ],

    "public_assistance": [
    "disasterNumber",
    "projectCategory",
    "obligatedAmount",
],

    # ✅ FIXED (NO $select)
    "disaster_summaries": None,
}

PAGE_SIZE = 1000
DATA_DIR = Path("data/raw")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

session = requests.Session()

# =====================================================
# HELPER
# =====================================================



def extract_records(payload):
    """
    FEMA API responses contain:
        metadata + dataset key
    We must extract ONLY the dataset list.
    """

    for key, value in payload.items():
        if isinstance(value, list):
            return value

    return []


def append_to_parquet(df, dataset_name):
    file_path = DATA_DIR / f"{dataset_name}.parquet"

    # If file exists → append
    if file_path.exists():
        existing = pd.read_parquet(file_path)
        df = pd.concat([existing, df], ignore_index=True)

    df.to_parquet(file_path, index=False)

# =====================================================
# STREAMING WRITER
# =====================================================

def stream_endpoint(name, endpoint, fields):

    url = endpoint["url"]
    params = {
        "$top": 1000,
        "$skip": 0,
    }

    # Only add $select if fields exist
    if fields:
       params["$select"] = ",".join([
    "DisasterNumber",
    "ProjectID",
    "ProjectCategory",
    "FederalShareObligated",
    "State"
])

    total = 0

    while True:
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            logging.warning(f"{name}: select failed → retrying without $select")
            params.pop("$select", None)

        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json().get("DisasterDeclarationsSummaries") \
            or response.json().get("PublicAssistanceFundedProjectsDetails") \
            or response.json().get("FemaWebDisasterSummaries") \
            or response.json().get("value")

        if not data:
            break

        df = pd.DataFrame(data)

        append_to_parquet(df, name)

        total += len(df)
        logging.info(f"{name}: wrote batch={len(df)} | total={total}")

        params["$skip"] += 1000

    logging.info(f"{name}: ingestion complete.")

# =====================================================
# MAIN PIPELINE
# =====================================================
def run_stream():
    logging.info("===== MEMORY SAFE FEMA STREAM STARTED =====")

    for name, endpoint in ENDPOINTS.items():
        try:
            stream_endpoint(name, endpoint, FIELDS[name])
        except Exception as e:
            logging.error(f"{name} failed → {e}")
            continue

    logging.info("===== INGESTION COMPLETE =====")   

def main():
    run_stream()

if __name__ == "__main__":
        main() 