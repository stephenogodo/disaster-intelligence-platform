"""
Final regime diagnostic for FEMA Disaster Recovery Cost Forecasting.

Purpose:
    Compare the current 23-feature XGBoost model on:
        1. All disasters
        2. Non-Biological disasters

This is a diagnostic only.
It does NOT modify the production model.
It does NOT modify features.csv.
It does NOT save or overwrite model artifacts.

Methodology:
    - Same 23 legitimate prediction features
    - Same random 80/20 split
    - Same XGBoost configuration used by the current model
    - Same random seed
    - Target: log_total_obligated_amount
"""

import logging

import numpy as np
import pandas as pd
import xgboost as xgb

from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    r2_score,
    mean_squared_error,
    mean_absolute_error,
)

from storage.config import PROCESSED_DATA_DIR


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


FEATURES = [
    "region_encoded",
    "incident_duration_days",
    "days_to_declaration",
    "state_disaster_frequency",
    "incident_severity_score",
    "declaration_year",
    "declaration_month_sin",
    "declaration_month_cos",
    "declaration_quarter_sin",
    "declaration_quarter_cos",
    "severity_x_duration",
    "severity_x_frequency",
    "duration_x_days_to_declaration",
    "is_fire",
    "is_severe_storm",
    "is_flood",
    "is_hurricane",
    "is_tornado",
    "is_snowstorm",
    "is_biological",
    "is_severe_ice_storm",
    "is_typhoon",
    "is_drought",
]

TARGET = "log_total_obligated_amount"

RANDOM_STATE = 42
TEST_SIZE = 0.20


def build_model():
    """Return the XGBoost configuration used by the current model."""

    return xgb.XGBRegressor(
        n_estimators=500,
        max_depth=6,
        learning_rate=0.01,
        subsample=0.6,
        colsample_bytree=1.0,
        min_child_weight=1,
        gamma=0.3,
        reg_alpha=0,
        reg_lambda=0.5,
        random_state=RANDOM_STATE,
    )


def evaluate_regime(df, regime_name):
    """Train and evaluate one regime."""

    available_features = [
        feature for feature in FEATURES
        if feature in df.columns
    ]

    data = df.dropna(
        subset=available_features + [TARGET]
    ).copy()

    X = data[available_features]
    y = data[TARGET]

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=TEST_SIZE,
        random_state=RANDOM_STATE,
    )

    logger.info("")
    logger.info("=" * 70)
    logger.info(f"{regime_name.upper()} REGIME")
    logger.info("=" * 70)
    logger.info(f"Samples: {len(data)}")
    logger.info(f"Train:   {len(X_train)}")
    logger.info(f"Test:    {len(X_test)}")

    model = build_model()

    logger.info("Training XGBoost...")

    model.fit(X_train, y_train)

    predictions = model.predict(X_test)

    r2 = r2_score(y_test, predictions)
    rmse = np.sqrt(
        mean_squared_error(y_test, predictions)
    )
    mae = mean_absolute_error(
        y_test,
        predictions,
    )

    logger.info("")
    logger.info("RESULTS")
    logger.info("-" * 70)
    logger.info(f"R²   = {r2:.4f}")
    logger.info(f"RMSE = {rmse:.4f}")
    logger.info(f"MAE  = {mae:.4f}")

    return {
        "regime": regime_name,
        "samples": len(data),
        "train": len(X_train),
        "test": len(X_test),
        "r2": r2,
        "rmse": rmse,
        "mae": mae,
    }


def main():

    logger.info("=" * 70)
    logger.info("FINAL FEMA COST MODEL — REGIME COMPARISON")
    logger.info("=" * 70)

    data_path = PROCESSED_DATA_DIR / "features.csv"

    logger.info(f"Loading: {data_path}")

    df = pd.read_csv(
        data_path,
        low_memory=False,
    )

    logger.info(f"Total records: {len(df)}")

    results = []

    # ---------------------------------------------------------
    # REGIME 1 — ALL DISASTERS
    # ---------------------------------------------------------

    results.append(
        evaluate_regime(
            df,
            "All Disasters",
        )
    )

    # ---------------------------------------------------------
    # REGIME 2 — NON-BIOLOGICAL DISASTERS
    # ---------------------------------------------------------

    non_biological = df[
        df["incidenttype"].ne("Biological")
    ].copy()

    results.append(
        evaluate_regime(
            non_biological,
            "Non-Biological Disasters",
        )
    )

    # ---------------------------------------------------------
    # COMPARISON
    # ---------------------------------------------------------

    results_df = pd.DataFrame(results)

    logger.info("")
    logger.info("=" * 70)
    logger.info("REGIME COMPARISON")
    logger.info("=" * 70)

    print(
        results_df[
            [
                "regime",
                "samples",
                "train",
                "test",
                "r2",
                "rmse",
                "mae",
            ]
        ].to_string(index=False)
    )

    logger.info("")
    logger.info("=" * 70)
    logger.info("DIAGNOSTIC COMPLETE")
    logger.info("=" * 70)
    logger.info(
        "No production model or dataset was modified."
    )


if __name__ == "__main__":
    main()