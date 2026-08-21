import numpy as np
import pandas as pd
import joblib

from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    r2_score,
)
from sklearn.model_selection import train_test_split

from storage.config import PROCESSED_DATA_DIR, PROJECT_ROOT


# ---------------------------------------------------------
# Load data
# ---------------------------------------------------------

DATA_PATH = PROCESSED_DATA_DIR / "features.csv"
MODEL_PATH = PROJECT_ROOT / "models" / "best_model.pkl"
FEATURE_PATH = PROJECT_ROOT / "models" / "feature_columns.pkl"

TARGET = "log_total_obligated_amount"

df = pd.read_csv(DATA_PATH, low_memory=False)

df = df.dropna(subset=[TARGET])

features = joblib.load(FEATURE_PATH)
model = joblib.load(MODEL_PATH)

X = df[features]
y = df[TARGET]


# ---------------------------------------------------------
# Reproduce the existing evaluation split
# ---------------------------------------------------------

X_train, X_test, y_train, y_test = train_test_split(
    X,
    y,
    test_size=0.20,
    random_state=42,
)


# ---------------------------------------------------------
# Predictions
# ---------------------------------------------------------

pred_log = model.predict(X_test)


# Convert log predictions back to dollars
actual_dollars = np.expm1(y_test)
predicted_dollars = np.expm1(pred_log)


# ---------------------------------------------------------
# Metrics
# ---------------------------------------------------------

log_r2 = r2_score(y_test, pred_log)
log_rmse = np.sqrt(mean_squared_error(y_test, pred_log))
log_mae = mean_absolute_error(y_test, pred_log)

dollar_mae = mean_absolute_error(
    actual_dollars,
    predicted_dollars,
)

dollar_rmse = np.sqrt(
    mean_squared_error(
        actual_dollars,
        predicted_dollars,
    )
)


# ---------------------------------------------------------
# Report
# ---------------------------------------------------------

print()
print("=" * 65)
print(" CURRENT FEMA COST MODEL — DOLLAR-SPACE EVALUATION")
print("=" * 65)

print()
print(f"Test samples       : {len(y_test):,}")

print()
print("LOG-SPACE METRICS")
print("-" * 65)
print(f"R²                 : {log_r2:.4f}")
print(f"RMSE               : {log_rmse:.4f}")
print(f"MAE                : {log_mae:.4f}")

print()
print("DOLLAR-SPACE METRICS")
print("-" * 65)
print(f"MAE                : ${dollar_mae:,.2f}")
print(f"RMSE               : ${dollar_rmse:,.2f}")

print()
print("ACTUAL VS PREDICTED")
print("-" * 65)
print(f"Actual median      : ${np.median(actual_dollars):,.2f}")
print(f"Predicted median   : ${np.median(predicted_dollars):,.2f}")
print(f"Actual mean        : ${np.mean(actual_dollars):,.2f}")
print(f"Predicted mean     : ${np.mean(predicted_dollars):,.2f}")

print()
print("=" * 65)