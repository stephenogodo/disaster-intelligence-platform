"""
Temporal validation for the FEMA Disaster Recovery Cost Forecasting model.

Purpose:
    Evaluate the current 23-feature Model A using a chronological
    train/test split rather than the production random 80/20 split.

Temporal boundary:
    Training: 1998-2022
    Test:     2023-2026

This script does NOT overwrite the production model.
"""

import logging
from pathlib import Path

import numpy as np
import pandas as pd
import xgboost as xgb

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

TRAIN_END_YEAR = 2022


def main():

    data_path = PROCESSED_DATA_DIR / "features.csv"

    logger.info("=" * 70)
    logger.info("TEMPORAL VALIDATION — FEMA COST FORECASTING")
    logger.info("=" * 70)

    logger.info(f"Loading data from {data_path}")

    df = pd.read_csv(data_path, low_memory=False)

    available_features = [
        feature for feature in FEATURES
        if feature in df.columns
    ]

    X = df[available_features]
    y = df[TARGET]

    train_mask = df["declaration_year"] <= TRAIN_END_YEAR
    test_mask = df["declaration_year"] > TRAIN_END_YEAR

    X_train = X.loc[train_mask]
    y_train = y.loc[train_mask]

    X_test = X.loc[test_mask]
    y_test = y.loc[test_mask]

    logger.info(
        f"Training period: "
        f"{df.loc[train_mask, 'declaration_year'].min()}–"
        f"{df.loc[train_mask, 'declaration_year'].max()}"
    )

    logger.info(
        f"Test period: "
        f"{df.loc[test_mask, 'declaration_year'].min()}–"
        f"{df.loc[test_mask, 'declaration_year'].max()}"
    )

    logger.info(
        f"Train samples: {len(X_train)} | "
        f"Temporal test samples: {len(X_test)}"
    )

    logger.info(
        f"Features used ({len(available_features)}): "
        f"{available_features}"
    )

    # Use the best XGBoost configuration identified by Model A.
    model = xgb.XGBRegressor(
        n_estimators=500,
        max_depth=7,
        learning_rate=0.01,
        subsample=0.7,
        colsample_bytree=0.7,
        min_child_weight=3,
        gamma=0,
        reg_alpha=0,
        reg_lambda=0.5,
        random_state=42,
    )

    logger.info("Training temporal XGBoost model...")

    model.fit(X_train, y_train)

    predictions = model.predict(X_test)

    r2 = r2_score(y_test, predictions)
    rmse = np.sqrt(mean_squared_error(y_test, predictions))
    mae = mean_absolute_error(y_test, predictions)

    logger.info("=" * 70)
    logger.info("TEMPORAL VALIDATION RESULTS")
    logger.info("=" * 70)
    logger.info(f"R²   = {r2:.4f}")
    logger.info(f"RMSE = {rmse:.4f}")
    logger.info(f"MAE  = {mae:.4f}")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()