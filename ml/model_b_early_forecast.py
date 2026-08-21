# ml/model_b_early_forecast.py
"""
Model B — Early Disaster Recovery Cost Forecaster

Purpose:
    Forecast recovery cost without using information that depends on
    the eventual incident duration.

Design:
    - No incident_duration_days
    - No severity_x_duration
    - No duration_x_days_to_declaration
    - Separate model artifacts from Model A
    - Initial evaluation uses the same random 80/20 split as Model A
    - Temporal validation will be performed separately

This script does NOT overwrite Model A artifacts.
"""

import logging
from pathlib import Path

import joblib
import mlflow
import mlflow.sklearn
import mlflow.xgboost
import numpy as np
import pandas as pd
import xgboost as xgb

from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    r2_score,
)
from sklearn.model_selection import RandomizedSearchCV, train_test_split

from storage.config import PROJECT_ROOT, PROCESSED_DATA_DIR


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

DATA_PATH = PROCESSED_DATA_DIR / "features.csv"
MODEL_DIR = PROJECT_ROOT / "models"

MODEL_B_PATH = MODEL_DIR / "model_b_early_forecast.pkl"
MODEL_B_FEATURES_PATH = MODEL_DIR / "model_b_feature_columns.pkl"


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_DIR = Path(__file__).resolve().parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "model_b_early_forecast.log"),
    ],
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Model B feature definition
# ---------------------------------------------------------------------------

# IMPORTANT:
# These are deliberately different from Model A.
#
# Removed:
#   incident_duration_days
#   severity_x_duration
#   duration_x_days_to_declaration
#
# Everything retained here must be available without knowing the eventual
# duration of the disaster.

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

RANDOM_STATE = 42

N_ITER_SEARCH = 50
CV_FOLDS = 5


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------

def evaluate(model, X_test, y_test):
    """Evaluate a model in log-cost space."""

    predictions = model.predict(X_test)

    return {
        "r2": round(r2_score(y_test, predictions), 4),
        "rmse": round(
            np.sqrt(mean_squared_error(y_test, predictions)),
            4,
        ),
        "mae": round(
            mean_absolute_error(y_test, predictions),
            4,
        ),
    }


# ---------------------------------------------------------------------------
# Tuned model helper
# ---------------------------------------------------------------------------

def tune_and_log(
    name,
    base_model,
    param_dist,
    X_train,
    X_test,
    y_train,
    y_test,
):
    logger.info(
        f"Tuning [{name}] via RandomizedSearchCV "
        f"({N_ITER_SEARCH} iterations, {CV_FOLDS}-fold CV)..."
    )

    search = RandomizedSearchCV(
        base_model,
        param_distributions=param_dist,
        n_iter=N_ITER_SEARCH,
        cv=CV_FOLDS,
        scoring="r2",
        random_state=RANDOM_STATE,
        n_jobs=-1,
        verbose=0,
    )

    search.fit(X_train, y_train)

    best_model = search.best_estimator_

    logger.info(
        f"[{name}] Best CV R²: {search.best_score_:.4f}"
    )

    logger.info(
        f"[{name}] Best params: {search.best_params_}"
    )

    metrics = evaluate(
        best_model,
        X_test,
        y_test,
    )

    with mlflow.start_run(run_name=f"Model_B_{name}_tuned"):

        mlflow.log_params(search.best_params_)

        mlflow.log_metric(
            "cv_r2",
            round(search.best_score_, 4),
        )

        mlflow.log_metrics(metrics)

        mlflow.set_tag(
            "model_type",
            name,
        )

        mlflow.set_tag(
            "project",
            "FEMA Cost Forecaster",
        )

        mlflow.set_tag(
            "model_version",
            "Model B — Early Forecast",
        )

        mlflow.set_tag(
            "duration_feature",
            "excluded",
        )

        mlflow.set_tag(
            "tuning",
            "RandomizedSearchCV",
        )

        if isinstance(
            best_model,
            xgb.XGBRegressor,
        ):
            mlflow.xgboost.log_model(
                best_model,
                artifact_path="model",
            )
        else:
            mlflow.sklearn.log_model(
                best_model,
                artifact_path="model",
            )

    logger.info(
        f"[{name}] Test set — "
        f"R²={metrics['r2']} | "
        f"RMSE={metrics['rmse']} | "
        f"MAE={metrics['mae']}"
    )

    return best_model, metrics


# ---------------------------------------------------------------------------
# Baseline helper
# ---------------------------------------------------------------------------

def train_baseline_and_log(
    name,
    model,
    params,
    X_train,
    X_test,
    y_train,
    y_test,
):
    logger.info(
        f"Training [{name}] baseline..."
    )

    model.fit(
        X_train,
        y_train,
    )

    metrics = evaluate(
        model,
        X_test,
        y_test,
    )

    with mlflow.start_run(
        run_name=f"Model_B_{name}"
    ):

        mlflow.log_params(params)

        mlflow.log_metrics(metrics)

        mlflow.set_tag(
            "model_type",
            name,
        )

        mlflow.set_tag(
            "project",
            "FEMA Cost Forecaster",
        )

        mlflow.set_tag(
            "model_version",
            "Model B — Early Forecast",
        )

        mlflow.set_tag(
            "duration_feature",
            "excluded",
        )

        mlflow.sklearn.log_model(
            model,
            artifact_path="model",
        )

    logger.info(
        f"[{name}] "
        f"R²={metrics['r2']} | "
        f"RMSE={metrics['rmse']} | "
        f"MAE={metrics['mae']}"
    )

    return model, metrics


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run():

    logger.info("=" * 70)
    logger.info(
        "MODEL B — EARLY DISASTER RECOVERY COST FORECAST"
    )
    logger.info("=" * 70)

    logger.info(
        f"Loading data from {DATA_PATH}"
    )

    df = pd.read_csv(
        DATA_PATH,
        low_memory=False,
    )

    df = df.dropna(
        subset=[TARGET]
    )

    available_features = [
        feature
        for feature in FEATURES
        if feature in df.columns
    ]

    missing_features = set(FEATURES) - set(
        available_features
    )

    if missing_features:
        logger.warning(
            f"Missing Model B features: {missing_features}"
        )

    X = df[available_features]
    y = df[TARGET]

    logger.info(
        f"Features used ({len(available_features)}): "
        f"{available_features}"
    )

    logger.info(
        f"Target: {TARGET} | Samples: {len(y)}"
    )

    # Explicit safety check.
    forbidden_features = {
        "incident_duration_days",
        "severity_x_duration",
        "duration_x_days_to_declaration",
    }

    accidentally_included = (
        forbidden_features.intersection(
            available_features
        )
    )

    if accidentally_included:
        raise ValueError(
            "Model B contains forbidden duration-dependent "
            f"features: {accidentally_included}"
        )

    logger.info(
        "Confirmed: no duration-dependent features included."
    )

    # -----------------------------------------------------------------------
    # Random 80/20 split
    # -----------------------------------------------------------------------

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=RANDOM_STATE,
    )

    logger.info(
        f"Train size: {len(X_train)} | "
        f"Test size: {len(X_test)}"
    )

    # -----------------------------------------------------------------------
    # MLflow
    # -----------------------------------------------------------------------

    mlflow_db = PROJECT_ROOT / "mlflow.db"

    db_uri = (
        f"sqlite:///{mlflow_db.as_posix()}"
    )

    logger.info(
        f"MLflow tracking URI: {db_uri}"
    )

    mlflow.set_tracking_uri(
        db_uri
    )

    mlflow.set_experiment(
        "FEMA_Cost_Forecasting"
    )

    # -----------------------------------------------------------------------
    # Train models
    # -----------------------------------------------------------------------

    all_results = {}
    trained_models = {}

    # Linear Regression
    lr_model, lr_metrics = train_baseline_and_log(
        "Linear Regression",
        LinearRegression(),
        {"fit_intercept": True},
        X_train,
        X_test,
        y_train,
        y_test,
    )

    all_results["Linear Regression"] = lr_metrics
    trained_models["Linear Regression"] = lr_model

    # -----------------------------------------------------------------------
    # Random Forest
    # -----------------------------------------------------------------------

    rf_param_dist = {
        "n_estimators": [
            100,
            200,
            300,
            400,
            500,
        ],
        "max_depth": [
            5,
            8,
            10,
            12,
            15,
            None,
        ],
        "min_samples_split": [
            2,
            5,
            10,
        ],
        "min_samples_leaf": [
            1,
            2,
            4,
        ],
        "max_features": [
            "sqrt",
            "log2",
            None,
        ],
    }

    rf_model, rf_metrics = tune_and_log(
        "Random Forest",
        RandomForestRegressor(
            random_state=RANDOM_STATE
        ),
        rf_param_dist,
        X_train,
        X_test,
        y_train,
        y_test,
    )

    all_results["Random Forest"] = rf_metrics
    trained_models["Random Forest"] = rf_model

    # -----------------------------------------------------------------------
    # XGBoost
    # -----------------------------------------------------------------------

    xgb_param_dist = {
        "n_estimators": [
            200,
            300,
            400,
            500,
            600,
        ],
        "max_depth": [
            3,
            4,
            5,
            6,
            7,
            8,
        ],
        "learning_rate": [
            0.01,
            0.03,
            0.05,
            0.07,
            0.1,
        ],
        "subsample": [
            0.6,
            0.7,
            0.8,
            0.9,
            1.0,
        ],
        "colsample_bytree": [
            0.6,
            0.7,
            0.8,
            0.9,
            1.0,
        ],
        "min_child_weight": [
            1,
            3,
            5,
            7,
        ],
        "gamma": [
            0,
            0.1,
            0.2,
            0.3,
        ],
        "reg_alpha": [
            0,
            0.01,
            0.1,
            1,
        ],
        "reg_lambda": [
            0.5,
            1,
            1.5,
            2,
        ],
    }

    xgb_model, xgb_metrics = tune_and_log(
        "XGBoost",
        xgb.XGBRegressor(
            random_state=RANDOM_STATE
        ),
        xgb_param_dist,
        X_train,
        X_test,
        y_train,
        y_test,
    )

    all_results["XGBoost"] = xgb_metrics
    trained_models["XGBoost"] = xgb_model

    # -----------------------------------------------------------------------
    # Select best Model B candidate
    # -----------------------------------------------------------------------

    best_name = max(
        all_results,
        key=lambda name: all_results[name]["r2"],
    )

    best_model = trained_models[
        best_name
    ]

    best_metrics = all_results[
        best_name
    ]

    logger.info(
        f"Model B candidate: [{best_name}] "
        f"with R²={best_metrics['r2']}"
    )

    # -----------------------------------------------------------------------
    # Save Model B ONLY
    # -----------------------------------------------------------------------

    MODEL_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    joblib.dump(
        best_model,
        MODEL_B_PATH,
    )

    joblib.dump(
        available_features,
        MODEL_B_FEATURES_PATH,
    )

    logger.info(
        f"Model B saved to: {MODEL_B_PATH}"
    )

    logger.info(
        f"Model B feature list saved to: "
        f"{MODEL_B_FEATURES_PATH}"
    )

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------

    logger.info("=" * 70)
    logger.info(
        "MODEL B COMPARISON SUMMARY"
    )
    logger.info("=" * 70)

    for name, metrics in all_results.items():
        logger.info(
            f"{name}: "
            f"R²={metrics['r2']} | "
            f"RMSE={metrics['rmse']} | "
            f"MAE={metrics['mae']}"
        )

    logger.info("=" * 70)
    logger.info(
        "MODEL B DEVELOPMENT COMPLETE"
    )
    logger.info("=" * 70)

    logger.info(
        "Model A artifacts were NOT modified."
    )


if __name__ == "__main__":
    run()