# ml/model_development.py
import logging
import numpy as np
import pandas as pd
import joblib
import mlflow
import mlflow.sklearn
import mlflow.xgboost
from pathlib import Path
from sklearn.model_selection import train_test_split, RandomizedSearchCV
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_error
import xgboost as xgb

from storage.config import (PROJECT_ROOT, PROCESSED_DATA_DIR)
# ── Paths ──────────────────────────────────────────────────────────────────────
data_path = PROCESSED_DATA_DIR / "features.csv"


# ── Logging ────────────────────────────────────────────────────────────────────
LOG_DIR = Path(__file__).resolve().parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "model_development.log"),
    ]
)
logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────
FEATURES = [
    "region_encoded", "incident_duration_days", "days_to_declaration",
    "state_disaster_frequency", "incident_severity_score", "declaration_year",
    "declaration_month_sin", "declaration_month_cos",
    "declaration_quarter_sin", "declaration_quarter_cos",
    "severity_x_duration", "severity_x_frequency", "duration_x_days_to_declaration",
    "is_fire", "is_severe_storm", "is_flood", "is_hurricane",
    "is_tornado", "is_snowstorm", "is_biological",
    "is_severe_ice_storm", "is_typhoon", "is_drought",
]
TARGET = "log_total_obligated_amount"

RANDOM_STATE = 42
N_ITER_SEARCH = 50   # RandomizedSearchCV iterations per model — lower this
                     # (e.g. 20) if a run takes too long on your machine
CV_FOLDS = 5


# ── Helpers ────────────────────────────────────────────────────────────────────
def evaluate(model, X_test, y_test) -> dict:
    preds = model.predict(X_test)
    return {
        "r2":   round(r2_score(y_test, preds), 4),
        "rmse": round(np.sqrt(mean_squared_error(y_test, preds)), 4),
        "mae":  round(mean_absolute_error(y_test, preds), 4),
    }


def tune_and_log(name, base_model, param_dist, X_train, X_test, y_train, y_test):
    logger.info(f"Tuning [{name}] via RandomizedSearchCV "
                f"({N_ITER_SEARCH} iterations, {CV_FOLDS}-fold CV)...")

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
    logger.info(f"[{name}] Best CV R²: {search.best_score_:.4f}")
    logger.info(f"[{name}] Best params: {search.best_params_}")

    with mlflow.start_run(run_name=f"{name}_tuned"):
        mlflow.log_params(search.best_params_)
        mlflow.log_metric("cv_r2", round(search.best_score_, 4))

        metrics = evaluate(best_model, X_test, y_test)
        mlflow.log_metrics(metrics)
        mlflow.set_tag("model_type", name)
        mlflow.set_tag("project", "FEMA Cost Forecaster")
        mlflow.set_tag("tuning", "RandomizedSearchCV")

        if isinstance(best_model, xgb.XGBRegressor):
            mlflow.xgboost.log_model(best_model, artifact_path="model")
        else:
            mlflow.sklearn.log_model(best_model, artifact_path="model")

        logger.info(
            f"[{name}] Test set — R²={metrics['r2']} | RMSE={metrics['rmse']} | MAE={metrics['mae']}"
        )

    return best_model, metrics


def train_baseline_and_log(name, model, params, X_train, X_test, y_train, y_test):
    """Untuned baseline — used for Linear Regression, which has little
    meaningful tuning surface and serves mainly as a reference point."""
    logger.info(f"Training [{name}] (baseline, no tuning)...")
    with mlflow.start_run(run_name=name):
        mlflow.log_params(params)
        model.fit(X_train, y_train)
        metrics = evaluate(model, X_test, y_test)
        mlflow.log_metrics(metrics)
        mlflow.set_tag("model_type", name)
        mlflow.set_tag("project", "FEMA Cost Forecaster")
        mlflow.sklearn.log_model(model, artifact_path="model")
        logger.info(f"[{name}] R²={metrics['r2']} | RMSE={metrics['rmse']} | MAE={metrics['mae']}")
    return model, metrics


# ── Main ───────────────────────────────────────────────────────────────────────
def run():
    logger.info("=== Model Development Started (with hyperparameter tuning) ===")
    data_path = PROCESSED_DATA_DIR / "features.csv"

    logger.info(f"Loading data from {data_path}")

    df = pd.read_csv(data_path, low_memory=False)
    df = df.dropna(subset=[TARGET])
    available_features = [f for f in FEATURES if f in df.columns]
    missing = set(FEATURES) - set(available_features)
    if missing:
        logger.warning(f"Missing features (will be skipped): {missing}")

    X = df[available_features]
    y = df[TARGET]
    logger.info(f"Features used ({len(available_features)}): {available_features}")
    logger.info(f"Target: {TARGET} | Samples: {len(y)}")

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=RANDOM_STATE
    )
    logger.info(f"Train size: {len(X_train)} | Test size: {len(X_test)}")

    MLFLOW_DB = PROJECT_ROOT / "mlflow.db"

    db_uri = f"sqlite:///{MLFLOW_DB.as_posix()}"
    logger.info(f"MLflow tracking URI: {db_uri}")
    mlflow.set_tracking_uri(db_uri)
    mlflow.set_experiment("FEMA_Cost_Forecasting")
    logger.info("MLflow experiment set: FEMA_Cost_Forecasting")

    all_results = {}
    trained_models = {}

    # --- Linear Regression: untuned baseline ---
    lr_model, lr_metrics = train_baseline_and_log(
        "Linear Regression", LinearRegression(), {"fit_intercept": True},
        X_train, X_test, y_train, y_test
    )
    all_results["Linear Regression"] = lr_metrics
    trained_models["Linear Regression"] = lr_model

    # --- Random Forest: tuned ---
    rf_param_dist = {
        "n_estimators": [100, 200, 300, 400, 500],
        "max_depth": [5, 8, 10, 12, 15, None],
        "min_samples_split": [2, 5, 10],
        "min_samples_leaf": [1, 2, 4],
        "max_features": ["sqrt", "log2", None],
    }
    rf_model, rf_metrics = tune_and_log(
        "Random Forest", RandomForestRegressor(random_state=RANDOM_STATE),
        rf_param_dist, X_train, X_test, y_train, y_test
    )
    all_results["Random Forest"] = rf_metrics
    trained_models["Random Forest"] = rf_model

    # --- XGBoost: tuned ---
    xgb_param_dist = {
        "n_estimators": [200, 300, 400, 500, 600],
        "max_depth": [3, 4, 5, 6, 7, 8],
        "learning_rate": [0.01, 0.03, 0.05, 0.07, 0.1],
        "subsample": [0.6, 0.7, 0.8, 0.9, 1.0],
        "colsample_bytree": [0.6, 0.7, 0.8, 0.9, 1.0],
        "min_child_weight": [1, 3, 5, 7],
        "gamma": [0, 0.1, 0.2, 0.3],
        "reg_alpha": [0, 0.01, 0.1, 1],
        "reg_lambda": [0.5, 1, 1.5, 2],
    }
    xgb_model, xgb_metrics = tune_and_log(
        "XGBoost", xgb.XGBRegressor(random_state=RANDOM_STATE),
        xgb_param_dist, X_train, X_test, y_train, y_test
    )
    all_results["XGBoost"] = xgb_metrics
    trained_models["XGBoost"] = xgb_model

    # --- Pick best model by R² ---
    best_name = max(all_results, key=lambda n: all_results[n]["r2"])
    best_model = trained_models[best_name]
    logger.info(f"Best model: [{best_name}] with R²={all_results[best_name]['r2']}")

    # --- Save ---
    model_dir = PROJECT_ROOT / "models"
    model_dir.mkdir(parents=True, exist_ok=True)
    model_dir.mkdir(exist_ok=True)
    joblib.dump(best_model, model_dir / "best_model.pkl")
    joblib.dump(available_features, model_dir / "feature_columns.pkl")
    logger.info(f"Best model saved to {model_dir / 'best_model.pkl'}")
    logger.info(f"Feature list saved to {model_dir / 'feature_columns.pkl'}")

    # --- Summary ---
    logger.info("=== Model Comparison Summary ===")
    for name, metrics in all_results.items():
        logger.info(f"  {name}: R²={metrics['r2']} | RMSE={metrics['rmse']} | MAE={metrics['mae']}")
    logger.info("=== Model Development Complete ===")


if __name__ == "__main__":
    run()
