"""
Disaster Intelligence Platform Launcher

Runs the Disaster Intelligence Platform
from a single entry point.

Author: Stephen Ogodo
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pandas as pd
# ==========================================================
# Utility Functions
# ==========================================================

def run_command(command: list[str], description: str) -> bool:
    """
    Execute a command.

    Returns
    -------
    bool
        True if successful.
    """

    print()
    print("=" * 70)
    print(description)
    print("=" * 70)

    result = subprocess.run(
        command,
        check=False,
    )

    if result.returncode != 0:

        print()
        print(f"ERROR: {description} failed.")

        return False

    print()
    print(f"SUCCESS: {description} completed.")
    return True
# ==========================================================
# Climate Data Validation / Preparation
# ==========================================================

def validate_climate_file(
    path: Path,
    required_columns: list[str],
) -> bool:
    """
    Validate that a climate dataset exists, is readable,
    non-empty, and contains the required columns.
    """

    if not path.exists():
        return False

    try:
        df = pd.read_parquet(path)

        if df.empty:
            return False

        return all(column in df.columns for column in required_columns)

    except Exception as exc:
        print(f"WARNING: Could not validate {path}: {exc}")
        return False


def ensure_climate_data() -> bool:
    """
    Ensure the climate datasets required by feature engineering
    are available and valid.

    Existing datasets are reused. Missing or invalid datasets
    are generated using the project's existing ingestion modules.
    """

    processed_dir = Path("data") / "processed"

    climate_path = processed_dir / "climate_aggregated.parquet"
    rainfall_path = processed_dir / "rainfall_aggregated.parquet"

    climate_valid = validate_climate_file(
        climate_path,
        [
            "disasternumber",
            "wind_speed",
            "flood_severity",
        ],
    )

    rainfall_valid = validate_climate_file(
        rainfall_path,
        [
            "disasternumber",
            "rainfall_intensity",
        ],
    )

    if climate_valid and rainfall_valid:
        print()
        print("Climate datasets are present and valid.")
        print(f"  Climate : {climate_path}")
        print(f"  Rainfall: {rainfall_path}")
        return True

    print()
    print("=" * 70)
    print("CLIMATE DATA PREPARATION")
    print("=" * 70)

    if not climate_valid:
        print("Climate dataset missing or invalid.")
        print("Generating NOAA Storm Events climate dataset...")

        if not run_command(
            [
                sys.executable,
                "-m",
                "data_ingestion.batch.climate_ingest",
            ],
            "Climate Data Preparation",
        ):
            return False

    if not rainfall_valid:
        print("Rainfall dataset missing or invalid.")
        print("Generating GHCN rainfall dataset...")

        if not run_command(
            [
                sys.executable,
                "-m",
                "data_ingestion.batch.rainfall_data_ingestion",
            ],
            "Rainfall Data Preparation",
        ):
            return False

    # ------------------------------------------------------
    # Final validation
    # ------------------------------------------------------

    climate_valid = validate_climate_file(
        climate_path,
        [
            "disasternumber",
            "wind_speed",
            "flood_severity",
        ],
    )

    rainfall_valid = validate_climate_file(
        rainfall_path,
        [
            "disasternumber",
            "rainfall_intensity",
        ],
    )

    if not climate_valid:
        print()
        print("ERROR: Climate dataset is still missing or invalid.")
        return False

    if not rainfall_valid:
        print()
        print("ERROR: Rainfall dataset is still missing or invalid.")
        return False

    print()
    print("SUCCESS: Climate datasets are ready.")

    return True

# ==========================================================
# Batch Processing Pipeline
# ==========================================================
# ==========================================================
# Batch Processing Pipeline
# ==========================================================

def run_batch_pipeline(model_version: str) -> bool:
    """
    Execute the complete batch workflow.

    Ingestion and model selection are intentionally independent:
    both Model A and Model B consume the same engineered dataset.
    """
    model_version = model_version.upper()
    if model_version not in {"A", "B"}:
        print(f"ERROR: Unsupported model version: {model_version}")
        return False

    print("\nStarting Batch Processing Pipeline...")
    print(f"Selected forecasting model: Model {model_version}")

    if not run_command(
        [sys.executable, "-m", "data_ingestion.common.incremental_update"],
        "STEP 1/4 - Incremental FEMA Update",
    ):
        return False

    print()
    print("=" * 70)
    print("STEP 2/4 - Ensure Climate Data")
    print("=" * 70)

    if not ensure_climate_data():
        print()
        print("ERROR: Required climate data is unavailable.")
        print("Batch pipeline stopped.")
        return False

    print()
    print("SUCCESS: STEP 2/4 - Climate Data ready.")

    if not run_command(
        [sys.executable, "-m", "feature_engineering.clean_and_engineer"],
        "STEP 3/4 - Shared Feature Engineering",
    ):
        return False

    model_module = (
        "ml.model_development"
        if model_version == "A"
        else "ml.model_b_early_forecast"
    )
    model_label = (
        "Model A - Standard Forecast"
        if model_version == "A"
        else "Model B - Early Forecast"
    )

    if not run_command(
        [sys.executable, "-m", model_module],
        f"STEP 4/4 - {model_label}",
    ):
        return False

    print()
    print(f"Batch pipeline completed successfully with {model_label}.")
    return True


# ==========================================================
# Kafka Streaming
# ==========================================================

def run_kafka_streaming(model_version: str) -> None:
    """
    Launch Kafka streaming.

    Kafka is an ingestion choice and does not determine the forecasting model.
    """
    model_version = model_version.upper()
    if model_version not in {"A", "B"}:
        print(f"ERROR: Unsupported model version: {model_version}")
        return

    print()
    print("Starting Kafka Consumer...")
    consumer = subprocess.Popen(
        [sys.executable, "-m", "data_ingestion.kafka.kafka_consumer"]
    )

    print("Starting Kafka Producer...")
    producer = subprocess.Popen(
        [sys.executable, "-m", "data_ingestion.kafka.kafka_producer"]
    )

    print()
    print("Kafka Streaming is now running.")
    print(f"Selected forecasting model: Model {model_version}")
    print("Press Ctrl+C to stop.")

    try:
        producer.wait()
        consumer.wait()
    except KeyboardInterrupt:
        print()
        print("Stopping Kafka Streaming...")
        producer.terminate()
        consumer.terminate()
        producer.wait()
        consumer.wait()
        print()
        print("Kafka Streaming stopped successfully.")


# ==========================================================
# User Interface Launcher
# ==========================================================

def launch_interface(model_version: str) -> None:
    """
    Launch the selected user interface.
    """
    model_version = model_version.upper()
    if model_version not in {"A", "B"}:
        print(f"ERROR: Unsupported model version: {model_version}")
        return

    process_env = __import__("os").environ.copy()
    process_env["MODEL_VERSION"] = model_version

    while True:

        print()
        print("=" * 70)
        print("SELECT OUTPUT INTERFACE")
        print("=" * 70)
        print("1. Launch Dashboard")
        print("2. Launch REST API")
        print("0. Return to Main Menu")

        choice = input("\nEnter your choice: ").strip()

        # --------------------------------------------------
        # Dashboard
        # --------------------------------------------------

        if choice == "1":

            print()
            print("Starting FastAPI backend...")

            api_process = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "uvicorn",
                    "api.main:app",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    "8000",
                ],
                env=process_env,
            )

            print("FastAPI backend started on http://127.0.0.1:8000")

            print()
            print("Starting Streamlit dashboard...")

            dashboard_process = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "streamlit",
                    "run",
                    "dashboard/app.py",
                ],
                env=process_env,
            )

            print()
            print("Dashboard and API are running.")
            print("Press Ctrl+C to stop both.")

            try:

                dashboard_process.wait()

            except KeyboardInterrupt:

                print()
                print("Stopping Dashboard and API...")

            finally:

                if dashboard_process.poll() is None:
                    dashboard_process.terminate()

                if api_process.poll() is None:
                    api_process.terminate()

                dashboard_process.wait()
                api_process.wait()

                print()
                print("Dashboard and API stopped.")

            return

        # --------------------------------------------------
        # REST API only
        # --------------------------------------------------

        elif choice == "2":

            subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "uvicorn",
                    "api.main:app",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    "8000",
                ],
                check=False,
            )

            return

        # --------------------------------------------------
        # Return
        # --------------------------------------------------

        elif choice == "0":

            return

        else:

            print("\nInvalid selection.")

# ==========================================================
# Main Menu
# ==========================================================

def select_ingestion() -> str:
    """Select the data-ingestion method independently of the model."""
    while True:
        print()
        print("=" * 70)
        print(" SELECT DATA INGESTION ")
        print("=" * 70)
        print("1. Batch")
        print("2. Kafka Streaming")
        print("0. Exit")

        choice = input("\nEnter your choice: ").strip()

        if choice == "1":
            return "batch"
        if choice == "2":
            return "kafka"
        if choice == "0":
            return ""

        print("\nInvalid selection. Please try again.")


def select_model() -> str:
    """Select the forecasting model independently of ingestion."""
    while True:
        print()
        print("=" * 70)
        print(" SELECT FORECASTING MODEL ")
        print("=" * 70)
        print("1. Model A - Standard Forecast")
        print("2. Model B - Early Forecast")
        print("0. Return to Ingestion Menu")

        choice = input("\nEnter your choice: ").strip()

        if choice == "1":
            return "A"
        if choice == "2":
            return "B"
        if choice == "0":
            return ""

        print("\nInvalid selection. Please try again.")


def main() -> None:
    """
    Main single-entry point.

    Ingestion and model selection are independent decisions.
    """
    while True:
        ingestion = select_ingestion()

        if not ingestion:
            print("\nGoodbye.")
            return

        model_version = select_model()

        if not model_version:
            continue

        if ingestion == "batch":
            success = run_batch_pipeline(model_version)
            if success:
                launch_interface(model_version)

        elif ingestion == "kafka":
            run_kafka_streaming(model_version)


# ==========================================================
# Program Entry Point
# ==========================================================

if __name__ == "__main__":
    main()