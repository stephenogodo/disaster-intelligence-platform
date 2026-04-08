import json
import time
import logging
import signal
import sys
from typing import Dict, List
from datetime import datetime, timezone
import requests
from kafka import KafkaProducer
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =====================================================
# CONFIGURATION
# =====================================================

BASE_URL = "https://www.fema.gov/api/open"

ENDPOINTS = {
    "declarations": f"{BASE_URL}/v2/DisasterDeclarationsSummaries",
    "public_assistance": f"{BASE_URL}/v2/PublicAssistanceFundedProjectsDetails",
    "disaster_summaries": f"{BASE_URL}/v1/FemaWebDisasterSummaries",
}

FIELDS: Dict[str, List[str]] = {
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
    "disaster_summaries": [
        "disasterNumber",
        "state",
        "incidentType",
        "incidentBeginDate",
        "incidentEndDate",
    ],
}

KAFKA_TOPIC = "fema_raw"
KAFKA_BOOTSTRAP = "localhost:9092"

PAGE_SIZE = 1000
REQUEST_TIMEOUT = 30
SLEEP_BETWEEN_REQUESTS = 1

MAX_RETRIES = 5
BACKOFF_FACTOR = 2

SCHEMA_VERSION = "1.0"

# =====================================================
# LOGGING
# =====================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("FEMA-Kafka-Producer")

# =====================================================
# GRACEFUL SHUTDOWN
# =====================================================

RUNNING = True


def shutdown_handler(sig, frame):
    global RUNNING
    logger.info("Shutdown signal received...")
    RUNNING = False


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

# =====================================================
# HTTP SESSION WITH RETRIES
# =====================================================

session = requests.Session()

retry_strategy = Retry(
    total=MAX_RETRIES,
    backoff_factor=BACKOFF_FACTOR,
    status_forcelist=[429, 500, 502, 503, 504],
)

adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)

# =====================================================
# KAFKA PRODUCER (TUNED)
# =====================================================

def on_send_success(metadata):
    logger.debug(
        f"Delivered → topic={metadata.topic} "
        f"partition={metadata.partition} offset={metadata.offset}"
    )


def on_send_error(excp):
    logger.error(f"Kafka delivery failed: {excp}")


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: str(k).encode("utf-8"),
    acks="all",
    retries=10,
    linger_ms=50,
    batch_size=32768,
    compression_type="gzip",
)

# =====================================================
# HELPERS
# =====================================================

def filter_fields(record: dict, dataset: str) -> dict:
    """Keep only required schema fields."""
    fields = FIELDS.get(dataset, [])
    return {k: record.get(k) for k in fields}


''' def extract_records(response_json: dict) -> List[dict]:
    """FEMA API wraps results in unknown key."""
    key = list(response_json.keys())[0]
    records = response_json.get(key, [])
    
    # Ensure each record is a dict
    cleaned = []
    for r in records:
        if isinstance(r, dict):
            cleaned.append(r)
        else:
            try:
                # Sometimes records may be JSON strings
                cleaned.append(json.loads(r))
            except Exception:
                # skip malformed record
                continue
    return cleaned '''


'''def extract_records(response_json: dict):

    for key, value in response_json.items():
        if isinstance(value, list):
            return value

    return []


def build_event(dataset: str, record: dict) -> dict:
    """Create standardized streaming event."""
    return {
        "schema_version": SCHEMA_VERSION,
        "source": dataset,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "payload": record,
    }


def send_to_kafka(dataset: str, record: dict):
    """Send event to Kafka and wait for delivery"""

    disaster_id = str(record.get("disasterNumber", "0"))

    event = build_event(dataset, record)

    future = producer.send(
        topic=KAFKA_TOPIC,
        key=disaster_id,
        value=event,
    )

    # ✅ BLOCK until Kafka confirms write
    metadata = future.get(timeout=60)

    logger.info(
        f"Delivered → topic={metadata.topic} "
        f"partition={metadata.partition} "
        f"offset={metadata.offset}"
    )


# =====================================================
# STREAM ONE DATASET
# =====================================================

def stream_dataset(name: str, url: str):

    logger.info(f"Starting stream for dataset: {name}")

    skip = 0
    total_sent = 0

    while RUNNING:
        params = {
            "$top": PAGE_SIZE,
            "$skip": skip,
            "$orderby": "declarationDate desc",
        }

        try:
            response = session.get(
                url,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()

            records = extract_records(response.json())
            logger.info(f"{name}: received {len(records)} records")

            if not records:
                logger.info(f"{name}: ingestion complete.there were no records to fetch OR there was no record fetched at all. Ending stream.")
                break

            for record in records:
                filtered = filter_fields(record, name)
                send_to_kafka(name, filtered)
                time.sleep(0.04)

            batch_size = len(records)
            total_sent += batch_size

            logger.info(
                f"{name}: sent batch={batch_size}, total={total_sent}"
            )

            skip += PAGE_SIZE
            time.sleep(SLEEP_BETWEEN_REQUESTS)

        except Exception as e:
            logger.error(f"{name}: streaming error -> {e}")
            time.sleep(5)

    logger.info(f"{name}: finished.")

# =====================================================
# MAIN PIPELINE
# =====================================================

def run_pipeline():

    logger.info("===== FEMA Kafka Producer Started =====")

    try:
        for dataset, url in ENDPOINTS.items():

            if not RUNNING:
                break

            stream_dataset(dataset, url)

    finally:
        logger.info("Flushing producer...")
        producer.flush()
        producer.close()
        logger.info("===== FEMA Kafka Producer Finished =====")

# =====================================================
# ENTRYPOINT
# =====================================================

if __name__ == "__main__":
    try:
        run_pipeline()
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        sys.exit(1)'''

        











import json
import time
import logging
import signal
import sys
from typing import Dict, List
from datetime import datetime, timezone
import os

import requests
from kafka import KafkaProducer
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ===============================
# CONFIGURATION (ENV-friendly)
# ===============================
BASE_URL = os.getenv("FEMA_BASE_URL", "https://www.fema.gov/api/open")

ENDPOINTS = {
    "declarations": f"{BASE_URL}/v2/DisasterDeclarationsSummaries",
    "public_assistance": f"{BASE_URL}/v2/PublicAssistanceFundedProjectsDetails",
    "disaster_summaries": f"{BASE_URL}/v1/FemaWebDisasterSummaries",
}

FIELDS: Dict[str, List[str]] = {
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
    "disaster_summaries": [
        "disasterNumber",
        "state",
        "incidentType",
        "incidentBeginDate",
        "incidentEndDate",
    ],
}

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "fema_raw")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
PAGE_SIZE = int(os.getenv("PAGE_SIZE", 1000))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", 30))
SLEEP_BETWEEN_REQUESTS = float(os.getenv("SLEEP_BETWEEN_REQUESTS", 1))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 5))
BACKOFF_FACTOR = float(os.getenv("BACKOFF_FACTOR", 2))
SCHEMA_VERSION = os.getenv("SCHEMA_VERSION", "1.0")

# ===============================
# LOGGING
# ===============================
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("FEMA-Kafka-Producer")

# ===============================
# GRACEFUL SHUTDOWN
# ===============================
RUNNING = True

def shutdown_handler(sig, frame):
    global RUNNING
    logger.info("Shutdown signal received...")
    RUNNING = False

signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

# ===============================
# HTTP SESSION WITH RETRIES
# ===============================
session = requests.Session()
retry_strategy = Retry(total=MAX_RETRIES, backoff_factor=BACKOFF_FACTOR,
                       status_forcelist=[429, 500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)

# ===============================
# KAFKA PRODUCER
# ===============================
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: str(k).encode("utf-8"),
    acks="all",
    retries=10,
    linger_ms=50,
    batch_size=32768,
    compression_type="gzip",
)

def build_event(dataset: str, record: dict) -> dict:
    """Create standardized streaming event."""
    return {
        "schema_version": SCHEMA_VERSION,
        "source": dataset,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "payload": record,
    }

def filter_fields(record: dict, dataset: str) -> dict:
    fields = FIELDS.get(dataset, [])
    return {k: record.get(k) for k in fields}

def send_to_kafka(dataset: str, record: dict):
    """Send event to Kafka synchronously to ensure delivery."""
    disaster_id = str(record.get("disasterNumber", "0"))
    event = build_event(dataset, record)
    future = producer.send(topic=KAFKA_TOPIC, key=disaster_id, value=event)
    try:
        future.get(timeout=60)
        logger.debug(f"Sent disasterNumber={disaster_id} to {KAFKA_TOPIC}")
    except Exception as e:
        logger.error(f"Failed to send disasterNumber={disaster_id}: {e}")

# ===============================
# STREAM DATASET
# ===============================
def stream_dataset(name: str, url: str):
    logger.info(f"Starting stream for dataset: {name}")
    skip = 0
    total_sent = 0

    while RUNNING:
        params = {"$top": PAGE_SIZE, "$skip": skip}
        try:
            response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            key = list(response.json().keys())[0]
            records = response.json().get(key, [])

            if not records:
                logger.info(f"{name}: ingestion complete.")
                break

            for record in records:
                filtered = filter_fields(record, name)
                send_to_kafka(name, filtered)
                time.sleep(0.04)  # optional small delay

            batch_size = len(records)
            total_sent += batch_size
            logger.info(f"{name}: sent batch={batch_size}, total={total_sent}")
            skip += PAGE_SIZE
            time.sleep(SLEEP_BETWEEN_REQUESTS)

        except Exception as e:
            logger.error(f"{name}: streaming error -> {e}")
            time.sleep(5)

    logger.info(f"{name}: finished.")

# ===============================
# MAIN PIPELINE
# ===============================
def run_pipeline():
    logger.info("===== FEMA Kafka Producer Started =====")
    try:
        for dataset, url in ENDPOINTS.items():
            if not RUNNING:
                break
            stream_dataset(dataset, url)
    finally:
        logger.info("Flushing producer...")
        producer.flush()
        producer.close()
        logger.info("===== FEMA Kafka Producer Finished =====")

# ===============================
# ENTRYPOINT
# ===============================
if __name__ == "__main__":
    try:
        run_pipeline()
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        sys.exit(1)