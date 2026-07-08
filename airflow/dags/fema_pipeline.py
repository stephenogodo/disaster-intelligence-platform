from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
import pandas as pd

from scripts.extract import fetch_all
from scripts.transform import clean_data
from scripts.features import build_features

# -----------------------------------
# CONFIG
# -----------------------------------
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

DATA_PATH = "/opt/airflow/data"

logger = logging.getLogger(__name__)

# -----------------------------------
# TASK 1: EXTRACT
# -----------------------------------
def extract_task(**context):

    url = Variable.get("FEMA_API_URL")

    logger.info("Starting data extraction...")

    data = fetch_all(url)

    if not data or not any(data.values()):
        raise ValueError("No data fetched from FEMA API")

    file_paths = {}

    execution_date = context["ds"]

    for key, records in data.items():

        if not records:
            logger.warning(f"No records for {key}")
            continue

        df = pd.DataFrame(records)

        file_path = f"{DATA_PATH}/{key}_raw_{execution_date}.parquet"

        df.to_parquet(file_path)

        logger.info(f"Saved {key} raw data: {file_path}")

        file_paths[key] = file_path

    return file_paths


# -----------------------------------
# TASK 2: TRANSFORM
# -----------------------------------
def transform_task(**context):

    ti = context["ti"]

    file_paths = ti.xcom_pull(task_ids="extract")

    cleaned_paths = {}

    execution_date = context["ds"]

    for key, raw_path in file_paths.items():

        logger.info(f"Processing {key} from {raw_path}")

        df = pd.read_parquet(raw_path)

        df = clean_data(df)

        if df.empty:
            logger.warning(f"Empty dataframe after cleaning for {key}")
            continue

        clean_path = f"{DATA_PATH}/{key}_clean_{execution_date}.parquet"

        df.to_parquet(clean_path)

        logger.info(f"Saved cleaned data: {clean_path}")

        cleaned_paths[key] = clean_path

    return cleaned_paths


# -----------------------------------
# TASK 3: FEATURE ENGINEERING
# -----------------------------------
def feature_task(**context):

    ti = context["ti"]

    file_paths = ti.xcom_pull(task_ids="transform")

    execution_date = context["ds"]

    for key, clean_path in file_paths.items():

        logger.info(f"Building features for {key}")

        df = pd.read_parquet(clean_path)

        df = build_features(df)

        if df.empty:
            logger.warning(f"No features generated for {key}")
            continue

        # Use DAG execution date for consistency
        df["event_timestamp"] = pd.to_datetime(execution_date)

        feature_path = f"{DATA_PATH}/{key}_features_{execution_date}.parquet"

        df.to_parquet(feature_path)

        logger.info(f"Saved features: {feature_path}")


# -----------------------------------
# DAG DEFINITION
# -----------------------------------
with DAG(
    dag_id="fema_pipeline",
    default_args=default_args,
    description="FEMA ETL + Feature Pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["fema", "ml", "production"],
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_task,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_task,
    )

    features = PythonOperator(
        task_id="features",
        python_callable=feature_task,
    )

    extract >> transform >> features
    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_task,
        provide_context=True,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_task,
        provide_context=True,
    )

    features = PythonOperator(
        task_id="features",
        python_callable=feature_task,
        provide_context=True,
    )

    extract >> transform >> features