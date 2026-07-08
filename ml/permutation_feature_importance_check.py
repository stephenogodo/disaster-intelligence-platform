# ml/permutation_importance_check.py
import logging
import pandas as pd
import numpy as np
import joblib
from pathlib import Path
from sklearn.model_selection import train_test_split
from sklearn.inspection import permutation_importance

LOG_DIR = Path(__file__).resolve().parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler(LOG_DIR / "permutation_importance.log")],
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
TARGET = "log_total_obligated_amount"

def run():
    logger.info("=== Permutation Importance Check Started ===")

    df = pd.read_csv(BASE_DIR / "direct_data_streaming" / "data" / "processed" / "features.csv", low_memory=False)
    df = df.dropna(subset=[TARGET])

    feature_cols = joblib.load(BASE_DIR / "models" / "feature_columns.pkl")
    model = joblib.load(BASE_DIR / "models" / "best_model.pkl")

    X = df[feature_cols]
    y = df[TARGET]

    # Same split as model_development.py (same random_state) so this is the SAME test set the model was evaluated on
    _, X_test, _, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    logger.info("Running permutation importance on held-out test set (n_repeats=20)...")
    result = permutation_importance(
        model, X_test, y_test, n_repeats=20, random_state=42, scoring="r2", n_jobs=-1
    )

    importance_df = pd.DataFrame({
        "feature": feature_cols,
        "importance_mean": result.importances_mean,
        "importance_std": result.importances_std,
    }).sort_values("importance_mean", ascending=False).reset_index(drop=True)

    logger.info("=== Permutation Importance Ranking (R² drop when shuffled) ===")
    for i, row in importance_df.iterrows():
        logger.info(f"{i+1:2d}. {row['feature']:30s} {row['importance_mean']:.4f} ± {row['importance_std']:.4f}")

    out_path = BASE_DIR / "reports" / "permutation_importance.csv"
    out_path.parent.mkdir(exist_ok=True)
    importance_df.to_csv(out_path, index=False)
    logger.info(f"Saved to {out_path}")
    logger.info("=== Complete ===")

if __name__ == "__main__":
    run()