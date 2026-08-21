"""
Temporal validation for Model B — Early Disaster Recovery Cost Forecast.

Purpose:
    Evaluate Model B using a chronological train/test split.

Model B deliberately excludes:
    - incident_duration_days
    - severity_x_duration
    - duration_x_days_to_declaration

Temporal boundary:
    Training: 1998–2022
    Test:     2023–2026

This script does NOT overwrite any production model artifacts.
"""

import logging

import numpy as np
import pandas as pd
import xgboost as xgb

from sklearn.metrics import (
    r2_score,
    mean_squared_error,
    mean_absolute_error,
)

from storage.config import PROCESSED_DATA_DIR


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Model B feature definition
# ---------------------------------------------------------------------------

FEATURES = [
    "region_encoded",
    "days_to_declaration",
    "state_disaster_frequency",
    "incident_severity_score",

    "declaration_year",
    "declaration_month_sin",
    "declaration_month_cos",
    "declaration_quarter_sin",
    "declaration_quarter_cos",

    "severity_x_frequency",

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


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():

    data_path = PROCESSED_DATA_DIR / "features.csv"

    logger.info("=" * 70)
    logger.info(
        "TEMPORAL VALIDATION — MODEL B EARLY COST FORECAST"
    )
    logger.info("=" * 70)

    logger.info(
        f"Loading data from {data_path}"
    )

    df = pd.read_csv(
        data_path,
        low_memory=False,
    )

    # -----------------------------------------------------------------------
    # Validate required columns
    # -----------------------------------------------------------------------

    required_columns = FEATURES + [
        TARGET,
        "declaration_year",
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            "Missing required columns: "
            f"{missing_columns}"
        )

    # -----------------------------------------------------------------------
    # Remove rows without target
    # -----------------------------------------------------------------------

    df = df.dropna(
        subset=[TARGET]
    ).copy()

    # -----------------------------------------------------------------------
    # Safety check:
    # Model B must never contain duration-dependent features.
    # -----------------------------------------------------------------------

    forbidden_features = {
        "incident_duration_days",
        "severity_x_duration",
        "duration_x_days_to_declaration",
    }

    accidentally_included = (
        forbidden_features.intersection(FEATURES)
    )

    if accidentally_included:
        raise ValueError(
            "Model B temporal validation contains forbidden "
            f"duration-dependent features: {accidentally_included}"
        )

    logger.info(
        f"Total usable records: {len(df)}"
    )

    # -----------------------------------------------------------------------
    # Prepare X and y
    # -----------------------------------------------------------------------

    X = df[FEATURES]
    y = df[TARGET]

    # -----------------------------------------------------------------------
    # Chronological split
    # -----------------------------------------------------------------------

    train_mask = (
        df["declaration_year"] <= TRAIN_END_YEAR
    )

    test_mask = (
        df["declaration_year"] > TRAIN_END_YEAR
    )

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
        f"Features used ({len(FEATURES)}): {FEATURES}"
    )

    # -----------------------------------------------------------------------
    # Model B XGBoost configuration
    #
    # These are the best XGBoost parameters found during Model B's
    # randomized search.
    # -----------------------------------------------------------------------

    model = xgb.XGBRegressor(
        n_estimators=500,
        max_depth=6,
        learning_rate=0.01,
        subsample=0.6,
        colsample_bytree=1.0,
        min_child_weight=1,
        gamma=0.3,
        reg_alpha=0,
        reg_lambda=0.5,
        random_state=42,
    )

    logger.info(
        "Training Model B temporal XGBoost..."
    )

    model.fit(
        X_train,
        y_train,
    )

    # -----------------------------------------------------------------------
    # Predict
    # -----------------------------------------------------------------------

    predictions = model.predict(
        X_test
    )

    # -----------------------------------------------------------------------
    # Metrics
    # -----------------------------------------------------------------------

    r2 = r2_score(
        y_test,
        predictions,
    )

    rmse = np.sqrt(
        mean_squared_error(
            y_test,
            predictions,
        )
    )

    mae = mean_absolute_error(
        y_test,
        predictions,
    )

    # -----------------------------------------------------------------------
    # Results
    # -----------------------------------------------------------------------

    logger.info("=" * 70)
    logger.info(
        "MODEL B TEMPORAL VALIDATION RESULTS"
    )
    logger.info("=" * 70)

    logger.info(
        f"R²   = {r2:.4f}"
    )

    logger.info(
        f"RMSE = {rmse:.4f}"
    )

    logger.info(
        f"MAE  = {mae:.4f}"
    )

    logger.info("=" * 70)

    logger.info(
        "Model B temporal validation complete."
    )

    logger.info(
        "No production model artifacts were modified."
    )


if __name__ == "__main__":
    main()