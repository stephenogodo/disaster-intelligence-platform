''' import json
import os
from datetime import datetime

import pandas as pd
from kafka import KafkaConsumer
from feast import FeatureStore

from scripts.transform import clean_data
from scripts.features import build_features

# -----------------------------------
# CONFIG
# -----------------------------------
KAFKA_TOPIC = "fema_raw"
BOOTSTRAP_SERVERS = "localhost:9092"
DATA_PATH = "data/features.parquet"
FEATURE_STORE_PATH = "feature_store/feature_repo"

BATCH_SIZE = 10

os.makedirs("data", exist_ok=True)

# -----------------------------------
# KAFKA CONSUMER
# -----------------------------------
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id="fema-group-dev",

    # Read existing + new messages
    auto_offset_reset="earliest",
    enable_auto_commit=True,

    # Stability settings
    session_timeout_ms=60000,
    heartbeat_interval_ms=15000,
    max_poll_interval_ms=300000,

    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

# -----------------------------------
# FEAST STORE
# -----------------------------------
store = FeatureStore(repo_path=FEATURE_STORE_PATH)

buffer = []

print("📥 FEMA Kafka Consumer started...")

# -----------------------------------
# STREAM LOOP
# -----------------------------------
for message in consumer:
    try:
        # ✅ Already deserialized
        record = message.value

        if not isinstance(record, dict):
            raise ValueError("Message is not a dict")

        buffer.append(record)

        # --------------------------
        # BATCH PROCESSING
        # --------------------------
        if len(buffer) >= BATCH_SIZE:

            df = pd.DataFrame(buffer)

            # CLEAN
            df = clean_data(df)

            # FEATURES
            df = build_features(df)

            # Feast timestamp column
            df["event_timestamp"] = datetime.utcnow()

            # --------------------------
            # SAVE OFFLINE STORE
            # --------------------------
            if os.path.exists(DATA_PATH):
                existing = pd.read_parquet(DATA_PATH)
                df = pd.concat([existing, df], ignore_index=True)

            df.to_parquet(DATA_PATH, index=False)

            print(f"✅ Saved {len(df)} records to offline store")

            # --------------------------
            # MATERIALIZE ONLINE STORE
            # --------------------------
            store.materialize_incremental(end_date=datetime.utcnow())

            print("⚡ Features materialized to Redis")

            buffer.clear()

    except Exception as e:
        print(f"❌ streaming error -> {e}") '''







''' import json
import logging
import os
from datetime import datetime

import pandas as pd
from kafka import KafkaConsumer

# ===============================
# CONFIG
# ===============================
BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "fema_raw"
GROUP_ID = "fema-stream"

FEATURE_PATH = "data/features.parquet"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

os.makedirs("data", exist_ok=True)

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

logging.info("===== FEMA Stream Processor Started =====")


# ===============================
# FEATURE ENGINEERING
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
# LOAD EXISTING DATASET
# ===============================
if os.path.exists(FEATURE_PATH):
    df_features = pd.read_parquet(FEATURE_PATH)
else:
    df_features = pd.DataFrame()

# ===============================
# STREAM LOOP
# ===============================
batch = []

for message in consumer:
    #record = message.value
    event = message.value
    record = event.get("payload", {})

    logging.info(
        f"Received → disaster={record.get('disasterNumber')} "
        f"state={record.get('state')}"
    )
    features = build_features(record)
    batch.append(features)

    # Save every 50 records
    if len(batch) >= 50:
        new_df = pd.DataFrame(batch)

        df_features = pd.concat(
            [df_features, new_df],
            ignore_index=True
        )

        df_features.to_parquet(FEATURE_PATH, index=False)

        logging.info(
            f"Saved {len(new_df)} records → {FEATURE_PATH}"
        )

        batch.clear() ''' 





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