# ml/inspect_feature_importance.py
import logging
import joblib
import pandas as pd
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
MODEL_PATH = BASE_DIR / "models" / "best_model.pkl"
FEATURES_PATH = BASE_DIR / "models" / "feature_columns.pkl"

# ── Logging ────────────────────────────────────────────────────────────────────
LOG_DIR = Path(__file__).resolve().parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "feature_importance.log"),
    ]
)
logger = logging.getLogger(__name__)


def run():
    logger.info("=== Feature Importance Inspection Started ===")

    model = joblib.load(MODEL_PATH)
    feature_cols = joblib.load(FEATURES_PATH)
    logger.info(f"Loaded model: {type(model).__name__}")

    if not hasattr(model, "feature_importances_"):
        logger.error(f"{type(model).__name__} does not expose feature_importances_")
        return

    importances = model.feature_importances_

    importance_df = pd.DataFrame({
        "feature": feature_cols,
        "importance": importances,
    }).sort_values("importance", ascending=False).reset_index(drop=True)

    importance_df["importance_pct"] = (importance_df["importance"] / importance_df["importance"].sum() * 100).round(2)

    logger.info("=== Feature Importance Ranking ===")
    for i, row in importance_df.iterrows():
        logger.info(f"{i+1:2d}. {row['feature']:30s} {row['importance']:.4f}  ({row['importance_pct']}%)")

    # --- Specific check: severity vs duration ---
    sev_rank = importance_df.index[importance_df["feature"] == "incident_severity_score"].tolist()
    dur_rank = importance_df.index[importance_df["feature"] == "incident_duration_days"].tolist()

    if sev_rank and dur_rank:
        sev_pos, dur_pos = sev_rank[0] + 1, dur_rank[0] + 1
        sev_imp = importance_df.loc[sev_rank[0], "importance_pct"]
        dur_imp = importance_df.loc[dur_rank[0], "importance_pct"]
        logger.info("=== Severity vs Duration Comparison ===")
        logger.info(f"incident_severity_score: rank #{sev_pos}, {sev_imp}% importance")
        logger.info(f"incident_duration_days:  rank #{dur_pos}, {dur_imp}% importance")
        if sev_imp > dur_imp:
            logger.info(f"CONFIRMED: severity ({sev_imp}%) dominates duration ({dur_imp}%) — consistent with sanity-check results")
        else:
            logger.info(f"UNEXPECTED: duration ({dur_imp}%) outweighs severity ({sev_imp}%) — investigate sanity-check results")

    # --- Save full table ---
    out_path = BASE_DIR / "reports" / "feature_importance.csv"
    out_path.parent.mkdir(exist_ok=True)
    importance_df.to_csv(out_path, index=False)
    logger.info(f"Full importance table saved to {out_path}")
    logger.info("=== Feature Importance Inspection Complete ===")

    return importance_df


if __name__ == "__main__":
    run()