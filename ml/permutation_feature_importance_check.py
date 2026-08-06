"""
Permutation Feature Importance Analysis
--------------------------------------

Evaluates the trained model using permutation importance on the held-out
test set and writes the feature ranking to:

    reports/permutation_importance.csv

This script uses the same feature list and trained model produced by
ml/model_development.py.
"""

import logging
from pathlib import Path

import joblib
import pandas as pd
from sklearn.inspection import permutation_importance
from sklearn.model_selection import train_test_split

from storage.config import PROJECT_ROOT, PROCESSED_DATA_DIR

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------

LOG_DIR = Path(__file__).resolve().parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "permutation_importance.log"),
    ],
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

TARGET = "log_total_obligated_amount"

MODEL_DIR = PROJECT_ROOT / "models"
REPORT_DIR = PROJECT_ROOT / "reports"

REPORT_DIR.mkdir(parents=True, exist_ok=True)


def run() -> None:
    """Compute permutation feature importance for the trained model."""

    logger.info("=== Permutation Importance Analysis Started ===")

    # -----------------------------------------------------------------
    # Load artefacts
    # -----------------------------------------------------------------

    df = pd.read_csv(
        PROCESSED_DATA_DIR / "features.csv",
        low_memory=False,
    ).dropna(subset=[TARGET])

    feature_cols = joblib.load(MODEL_DIR / "feature_columns.pkl")
    model = joblib.load(MODEL_DIR / "best_model.pkl")

    # -----------------------------------------------------------------
    # Recreate the evaluation split
    # -----------------------------------------------------------------

    X = df[feature_cols]
    y = df[TARGET]

    _, X_test, _, y_test = train_test_split(
        X,
        y,
        test_size=0.20,
        random_state=42,
    )

    logger.info("Running permutation importance (20 repeats)...")

    result = permutation_importance(
        model,
        X_test,
        y_test,
        n_repeats=20,
        random_state=42,
        scoring="r2",
        n_jobs=-1,
    )

    importance_df = (
        pd.DataFrame(
            {
                "feature": feature_cols,
                "importance_mean": result.importances_mean,
                "importance_std": result.importances_std,
            }
        )
        .sort_values("importance_mean", ascending=False)
        .reset_index(drop=True)
    )

    logger.info("=== Feature Importance Ranking ===")

    for idx, row in importance_df.iterrows():
        logger.info(
            "%2d. %-35s %.4f ± %.4f",
            idx + 1,
            row["feature"],
            row["importance_mean"],
            row["importance_std"],
        )

    output_file = REPORT_DIR / "permutation_importance.csv"
    importance_df.to_csv(output_file, index=False)

    logger.info("Results saved to %s", output_file)
    logger.info("=== Analysis Complete ===")


if __name__ == "__main__":
    run()