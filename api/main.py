# api/main.py
import logging
import math
import os
import numpy as np
import joblib
from pathlib import Path
from datetime import datetime
from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel, Field

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent

# Model A — Scenario Cost Estimator
MODEL_A_PATH = BASE_DIR / "models" / "best_model.pkl"
FEATURES_A_PATH = BASE_DIR / "models" / "feature_columns.pkl"

# Model B — Early Cost Forecaster
MODEL_B_PATH = BASE_DIR / "models" / "model_b_early_forecast.pkl"
FEATURES_B_PATH = BASE_DIR / "models" / "model_b_feature_columns.pkl"

# Default model for clients that do not explicitly select one.
# Dashboard/API clients can override this per request with X-Model-Version.
DEFAULT_MODEL_VERSION = os.environ.get("MODEL_VERSION", "B").strip().upper()
if DEFAULT_MODEL_VERSION not in {"A", "B"}:
    DEFAULT_MODEL_VERSION = "B"


# ── Logging ──────────────────────────────────────────────────────────────────────
LOG_DIR = Path(__file__).resolve().parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "api.log"),
    ],
)

logger = logging.getLogger(__name__)


# ── App ────────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="FEMA Disaster Cost Forecaster",
    description=(
        "FEMA disaster recovery cost forecasting API with "
        "scenario estimation and early forecasting modes."
    ),
    version="2.0.0",
)


# ── Model artifacts ─────────────────────────────────────────────────────────────
model_a = None
feature_cols_a = None

model_b = None
feature_cols_b = None


@app.on_event("startup")
def load_artifacts():
    global model_a, feature_cols_a
    global model_b, feature_cols_b

    logger.info("=== API Startup: Loading model artifacts ===")

    try:
        # ---------------------------------------------------------------------
        # Model A
        # ---------------------------------------------------------------------

        if not MODEL_A_PATH.exists():
            raise FileNotFoundError(
                f"Model A not found at {MODEL_A_PATH}"
            )

        if not FEATURES_A_PATH.exists():
            raise FileNotFoundError(
                f"Model A feature list not found at {FEATURES_A_PATH}"
            )

        model_a = joblib.load(MODEL_A_PATH)
        feature_cols_a = joblib.load(FEATURES_A_PATH)

        logger.info(
            f"Model A loaded: {type(model_a).__name__}"
        )

        logger.info(
            f"Model A features ({len(feature_cols_a)}): "
            f"{feature_cols_a}"
        )

        # ---------------------------------------------------------------------
        # Model B
        # ---------------------------------------------------------------------

        if not MODEL_B_PATH.exists():
            raise FileNotFoundError(
                f"Model B not found at {MODEL_B_PATH}"
            )

        if not FEATURES_B_PATH.exists():
            raise FileNotFoundError(
                f"Model B feature list not found at {FEATURES_B_PATH}"
            )

        model_b = joblib.load(MODEL_B_PATH)
        feature_cols_b = joblib.load(FEATURES_B_PATH)

        logger.info(
            f"Model B loaded: {type(model_b).__name__}"
        )

        logger.info(
            f"Model B features ({len(feature_cols_b)}): "
            f"{feature_cols_b}"
        )

        # ---------------------------------------------------------------------
        # Safety check for Model B
        # ---------------------------------------------------------------------

        forbidden_b_features = {
            "incident_duration_days",
            "severity_x_duration",
            "duration_x_days_to_declaration",
        }

        forbidden_found = (
            forbidden_b_features.intersection(
                feature_cols_b
            )
        )

        if forbidden_found:
            raise ValueError(
                "Model B contains forbidden duration-dependent "
                f"features: {forbidden_found}"
            )

        logger.info(
            "Model B safety check passed: "
            "no duration-dependent features."
        )

        logger.info(
            "=== All model artifacts loaded successfully ==="
        )

    except Exception as e:
        logger.error(
            f"Failed to load model artifacts: {e}"
        )
        raise


# ── Shared response schema ──────────────────────────────────────────────────────
class PredictionResponse(BaseModel):
    predicted_recovery_cost_usd: float
    log_prediction: float
    model_type: str
    forecast_type: str
    forecast_mode: str
    timestamp: str


# ── Model A request schema ──────────────────────────────────────────────────────
class DisasterInput(BaseModel):
    model_config = {"extra": "forbid"}

    """
    Model A — Scenario Cost Estimator.

    Duration is intentionally required because this model answers
    a scenario-based question.
    """

    region_encoded: int = Field(
        ...,
        description="Encoded FEMA region",
    )

    incident_duration_days: float = Field(
        ...,
        ge=0,
        description="Assumed incident duration in days",
    )

    days_to_declaration: float = Field(
        ...,
        ge=0,
        description="Days from incident start to declaration",
    )

    state_disaster_frequency: float = Field(
        ...,
        ge=0,
        description="Prior disaster count for the state",
    )

    incident_severity_score: float = Field(
        ...,
        ge=0,
        le=1,
        description="Computed severity score",
    )

    declaration_year: int = Field(
        ...,
        ge=1950,
        le=2100,
    )

    declaration_month: int = Field(
        ...,
        ge=1,
        le=12,
    )

    declaration_quarter: int = Field(
        ...,
        ge=1,
        le=4,
    )

    is_fire: int = Field(0, ge=0, le=1)
    is_severe_storm: int = Field(0, ge=0, le=1)
    is_flood: int = Field(0, ge=0, le=1)
    is_hurricane: int = Field(0, ge=0, le=1)
    is_tornado: int = Field(0, ge=0, le=1)
    is_snowstorm: int = Field(0, ge=0, le=1)
    is_biological: int = Field(0, ge=0, le=1)
    is_severe_ice_storm: int = Field(0, ge=0, le=1)
    is_typhoon: int = Field(0, ge=0, le=1)
    is_drought: int = Field(0, ge=0, le=1)


# ── Model B request schema ──────────────────────────────────────────────────────
class EarlyForecastInput(BaseModel):
    model_config = {"extra": "forbid"}

    """
    Model B — Early Disaster Cost Forecaster.

    IMPORTANT:
        No incident duration is accepted.
        No duration-derived feature is accepted.
    """

    region_encoded: int = Field(
        ...,
        description="Encoded FEMA region",
    )

    days_to_declaration: float = Field(
        ...,
        ge=0,
        description="Days from incident start to declaration",
    )

    state_disaster_frequency: float = Field(
        ...,
        ge=0,
        description="Prior disaster count for the state",
    )

    incident_severity_score: float = Field(
        ...,
        ge=0,
        le=1,
        description="Available incident severity assessment",
    )

    declaration_year: int = Field(
        ...,
        ge=1950,
        le=2100,
    )

    declaration_month: int = Field(
        ...,
        ge=1,
        le=12,
    )

    declaration_quarter: int = Field(
        ...,
        ge=1,
        le=4,
    )

    is_fire: int = Field(0, ge=0, le=1)
    is_severe_storm: int = Field(0, ge=0, le=1)
    is_flood: int = Field(0, ge=0, le=1)
    is_hurricane: int = Field(0, ge=0, le=1)
    is_tornado: int = Field(0, ge=0, le=1)
    is_snowstorm: int = Field(0, ge=0, le=1)
    is_biological: int = Field(0, ge=0, le=1)
    is_severe_ice_storm: int = Field(0, ge=0, le=1)
    is_typhoon: int = Field(0, ge=0, le=1)
    is_drought: int = Field(0, ge=0, le=1)


# ── Health endpoint ─────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    status = (
        "ok"
        if model_a is not None and model_b is not None
        else "models_not_loaded"
    )

    logger.info(
        f"Health check: {status}"
    )

    return {
        "status": status,
        "model_a_loaded": model_a is not None,
        "model_b_loaded": model_b is not None,
        "default_model_version": DEFAULT_MODEL_VERSION,
        "available_models": ["A", "B"],
    }


# ── Unified prediction endpoint ────────────────────────────────────────────────
@app.post(
    "/predict-cost",
    response_model=PredictionResponse,
)
def predict_cost(
    data: dict,
    x_model_version: str | None = Header(
        default=None,
        alias="X-Model-Version",
    ),
):
    """
    Unified prediction endpoint.

    Model selection is independent of data ingestion. A client may select:
      - Model A: Standard/scenario forecast; duration is required.
      - Model B: Early forecast; duration is forbidden.

    Selection precedence:
      1. X-Model-Version request header
      2. MODEL_VERSION environment variable
      3. B as the safe default
    """
    selected_version = (
        x_model_version.strip().upper()
        if x_model_version
        else DEFAULT_MODEL_VERSION
    )

    if selected_version not in {"A", "B"}:
        raise HTTPException(
            status_code=400,
            detail="Invalid model version. Use 'A' or 'B'.",
        )

    logger.info(
        f"Prediction request received | model={selected_version} | "
        f"fields={list(data.keys())}"
    )

    try:
        if selected_version == "A":
            # Model A explicitly requires duration.
            request = DisasterInput(**data)
            return _predict_model_a(request)

        # Model B must not receive duration or any duration-derived feature.
        forbidden = {
            "incident_duration_days",
            "severity_x_duration",
            "duration_x_days_to_declaration",
        }
        supplied_forbidden = forbidden.intersection(data.keys())

        if supplied_forbidden:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Model B does not accept duration-dependent fields: "
                    f"{sorted(supplied_forbidden)}"
                ),
            )

        request = EarlyForecastInput(**data)
        return _predict_model_b(request)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            f"Model {selected_version} request validation/prediction failed: {e}"
        )
        raise HTTPException(status_code=400, detail=str(e))


def _predict_model_a(data: DisasterInput):
    if model_a is None or feature_cols_a is None:
        logger.error("Model A prediction requested but model is not loaded")
        raise HTTPException(
            status_code=503,
            detail="Model A not loaded. Check server logs.",
        )

    logger.info(f"Model A prediction request: {data.model_dump() if hasattr(data, 'model_dump') else data.dict()}")

    try:
        input_dict = data.model_dump() if hasattr(data, "model_dump") else data.dict()

        month = input_dict["declaration_month"]
        quarter = input_dict["declaration_quarter"]

        input_dict["declaration_month_sin"] = math.sin(
            2 * math.pi * month / 12
        )
        input_dict["declaration_month_cos"] = math.cos(
            2 * math.pi * month / 12
        )
        input_dict["declaration_quarter_sin"] = math.sin(
            2 * math.pi * quarter / 4
        )
        input_dict["declaration_quarter_cos"] = math.cos(
            2 * math.pi * quarter / 4
        )

        severity = input_dict["incident_severity_score"]
        duration = input_dict["incident_duration_days"]
        frequency = input_dict["state_disaster_frequency"]
        days_to_decl = input_dict["days_to_declaration"]

        input_dict["severity_x_duration"] = severity * duration
        input_dict["severity_x_frequency"] = severity * frequency
        input_dict["duration_x_days_to_declaration"] = duration * days_to_decl

        features = [input_dict[col] for col in feature_cols_a]

        log_pred = float(model_a.predict([features])[0])
        predicted_cost = float(np.expm1(log_pred))

        response = PredictionResponse(
            predicted_recovery_cost_usd=round(predicted_cost, 2),
            log_prediction=round(log_pred, 4),
            model_type="Model A — Standard Forecast",
            forecast_type="Standard Forecast",
            forecast_mode="scenario",
            timestamp=datetime.utcnow().isoformat(),
        )

        logger.info(f"Model A prediction successful: ${predicted_cost:,.2f}")
        return response

    except KeyError as e:
        logger.error(f"Model A missing feature: {e}")
        raise HTTPException(
            status_code=400,
            detail=f"Missing or mismatched feature: {e}",
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Model A prediction failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def _predict_model_b(data: EarlyForecastInput):
    if model_b is None or feature_cols_b is None:
        logger.error("Model B forecast requested but model is not loaded")
        raise HTTPException(
            status_code=503,
            detail="Model B not loaded. Check server logs.",
        )

    logger.info(
        f"Model B early forecast request: "
        f"{data.model_dump() if hasattr(data, 'model_dump') else data.dict()}"
    )

    try:
        input_dict = data.model_dump() if hasattr(data, "model_dump") else data.dict()

        month = input_dict["declaration_month"]
        quarter = input_dict["declaration_quarter"]

        input_dict["declaration_month_sin"] = math.sin(
            2 * math.pi * month / 12
        )
        input_dict["declaration_month_cos"] = math.cos(
            2 * math.pi * month / 12
        )
        input_dict["declaration_quarter_sin"] = math.sin(
            2 * math.pi * quarter / 4
        )
        input_dict["declaration_quarter_cos"] = math.cos(
            2 * math.pi * quarter / 4
        )

        severity = input_dict["incident_severity_score"]
        frequency = input_dict["state_disaster_frequency"]

        input_dict["severity_x_frequency"] = severity * frequency

        forbidden_features = {
            "incident_duration_days",
            "severity_x_duration",
            "duration_x_days_to_declaration",
        }

        if forbidden_features.intersection(input_dict.keys()):
            raise ValueError(
                "Duration-dependent data detected in Model B request."
            )

        features = [input_dict[col] for col in feature_cols_b]

        log_pred = float(model_b.predict([features])[0])
        predicted_cost = float(np.expm1(log_pred))

        response = PredictionResponse(
            predicted_recovery_cost_usd=round(predicted_cost, 2),
            log_prediction=round(log_pred, 4),
            model_type="Model B — Early Forecast",
            forecast_type="Early Forecast",
            forecast_mode="early_forecast",
            timestamp=datetime.utcnow().isoformat(),
        )

        logger.info(f"Model B forecast successful: ${predicted_cost:,.2f}")
        return response

    except KeyError as e:
        logger.error(f"Model B missing feature: {e}")
        raise HTTPException(
            status_code=400,
            detail=f"Missing or mismatched feature: {e}",
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Model B forecast failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ── Backwards-compatible Model B endpoint ─────────────────────────────────────
@app.post(
    "/forecast-cost",
    response_model=PredictionResponse,
)
def forecast_cost(data: EarlyForecastInput):
    """
    Backwards-compatible Model B endpoint.

    New clients should use /predict-cost with X-Model-Version: B.
    """
    return _predict_model_b(data)


# ── Root ────────────────────────────────────────────────────────────────────────
@app.get("/")
def root():
    return {
        "message": "FEMA Disaster Cost Forecaster API",
        "endpoints": [
            "/health",
            "/predict-cost",
            "/forecast-cost",
            "/docs",
        ],
    }