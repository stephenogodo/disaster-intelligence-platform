import json
import logging
import os
from datetime import datetime

import pandas as pd
from kafka import KafkaConsumer

# ===============================
# CONFIGURATION
# ===============================
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "fema_raw")
GROUP_ID = os.getenv("KAFKA_GROUP", "fema-stream")
FEATURE_PATH = os.getenv("FEATURE_PATH", "data/features.parquet")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 500))

# Ensure data folder exists
os.makedirs(os.path.dirname(FEATURE_PATH), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("FEMA-Kafka-Consumer")

# ===============================
# LOAD EXISTING DATASET
# ===============================
if os.path.exists(FEATURE_PATH):
    df_features = pd.read_parquet(FEATURE_PATH)
    # Track already processed disasterNumbers
    processed_ids = set(df_features["disasterNumber"].astype(str))
    logger.info(f"Loaded existing dataset with {len(processed_ids)} records")
else:
    df_features = pd.DataFrame()
    processed_ids = set()
    logger.info("No existing dataset found. Starting fresh.")

# ===============================
# KAFKA CONSUMER
# ===============================
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id=GROUP_ID,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    session_timeout_ms=60000,
    heartbeat_interval_ms=15000,
    max_poll_interval_ms=300000,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

logger.info("===== FEMA Stream Processor Started =====")

# ===============================
# FEATURE ENGINEERING FUNCTION
# ===============================
def build_features(record: dict) -> dict:
    """
    Transform raw FEMA record → ML features
    """
    return {
        "disasterNumber": record.get("disasterNumber"),
        "state": record.get("state"),
        "incidentType": record.get("incidentType"),
        "declarationDate": record.get("declarationDate"),
        "year": (
            pd.to_datetime(record.get("declarationDate")).year
            if record.get("declarationDate")
            else None
        ),
        "ingested_at": datetime.utcnow(),
    }

# ===============================
# STREAM LOOP
# ===============================
batch = []

try:
    for message in consumer:
        record = message.value
        disaster_id = str(record.get("disasterNumber"))

        # Skip already processed records
        if disaster_id in processed_ids:
            continue

        features = build_features(record)
        batch.append(features)
        processed_ids.add(disaster_id)

        # Save batch to Parquet when batch size is reached
        if len(batch) >= BATCH_SIZE:
            new_df = pd.DataFrame(batch)
            df_features = pd.concat([df_features, new_df], ignore_index=True)
            df_features.to_parquet(FEATURE_PATH, index=False, compression="gzip")
            logger.info(f"Saved {len(new_df)} new records → {FEATURE_PATH}")
            batch.clear()

except KeyboardInterrupt:
    logger.info("Stream interrupted by user")

finally:
    # Save any remaining records on exit
    if batch:
        new_df = pd.DataFrame(batch)
        df_features = pd.concat([df_features, new_df], ignore_index=True)
        df_features.to_parquet(FEATURE_PATH, index=False, compression="gzip")
        logger.info(f"Saved {len(new_df)} remaining records → {FEATURE_PATH}")
    logger.info("===== FEMA Stream Processor Finished =====")