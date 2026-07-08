# backend/main.py
import logging
import math
import numpy as np
import joblib
from pathlib import Path
from datetime import datetime
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

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
        logging.FileHandler(LOG_DIR / "api.log"),
    ]
)
logger = logging.getLogger(__name__)

# ── App ────────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="FEMA Disaster Cost Forecaster",
    description="Predicts disaster recovery costs at point of declaration",
    version="1.0.0",
)

# ── Load model artifacts at startup ──────────────────────────────────────────────
model = None
feature_cols = None

@app.on_event("startup")
def load_artifacts():
    global model, feature_cols
    logger.info("=== API Startup: Loading model artifacts ===")
    try:
        if not MODEL_PATH.exists():
            raise FileNotFoundError(f"Model not found at {MODEL_PATH}")
        if not FEATURES_PATH.exists():
            raise FileNotFoundError(f"Feature list not found at {FEATURES_PATH}")

        model = joblib.load(MODEL_PATH)
        feature_cols = joblib.load(FEATURES_PATH)
        logger.info(f"Model loaded: {type(model).__name__}")
        logger.info(f"Feature columns ({len(feature_cols)}): {feature_cols}")
    except Exception as e:
        logger.error(f"Failed to load model artifacts: {e}")
        raise


# ── Request schema ────────────────────────────────────────────────────────────
class DisasterInput(BaseModel):
    region_encoded: int = Field(..., description="Encoded FEMA region")
    incident_duration_days: float = Field(..., ge=0, description="Length of incident in days")
    days_to_declaration: float = Field(..., ge=0, description="Days from incident start to declaration")
    state_disaster_frequency: float = Field(..., ge=0, description="Prior disaster count for the state")
    incident_severity_score: float = Field(..., description="Computed severity score")
    declaration_year: int = Field(..., ge=1950, le=2100)
    declaration_month: int = Field(..., ge=1, le=12)
    declaration_quarter: int = Field(..., ge=1, le=4)
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

    class Config:
        json_schema_extra = {
            "example": {
                "region_encoded": 4,
                "incident_duration_days": 14,
                "days_to_declaration": 7,
                "state_disaster_frequency": 12,
                "incident_severity_score": 0.62,
                "declaration_year": 2026,
                "declaration_month": 9,
                "declaration_quarter": 3,
                "is_fire": 0,
                "is_severe_storm": 0,
                "is_flood": 1,
                "is_hurricane": 1,
                "is_tornado": 0,
                "is_snowstorm": 0,
                "is_biological": 0,
                "is_severe_ice_storm": 0,
                "is_typhoon": 0,
                "is_drought": 0,
            }
        }


class PredictionResponse(BaseModel):
    predicted_recovery_cost_usd: float
    log_prediction: float
    model_type: str
    timestamp: str


# ── Endpoints ──────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    status = "ok" if model is not None else "model_not_loaded"
    logger.info(f"Health check: {status}")
    return {"status": status, "model_loaded": model is not None}




@app.post("/predict-cost", response_model=PredictionResponse)
def predict_cost(data: DisasterInput):
    if model is None or feature_cols is None:
        logger.error("Prediction requested but model is not loaded")
        raise HTTPException(status_code=503, detail="Model not loaded. Check server logs.")

    logger.info(f"Prediction request received: {data.dict()}")

    try:
        input_dict = data.dict()

        # Derive engineered features server-side — must exactly match what
        # clean_and_engineer.py computes during training, or predictions
        # will use misaligned/garbage values.
        month = input_dict["declaration_month"]
        quarter = input_dict["declaration_quarter"]
        input_dict["declaration_month_sin"] = math.sin(2 * math.pi * month / 12)
        input_dict["declaration_month_cos"] = math.cos(2 * math.pi * month / 12)
        input_dict["declaration_quarter_sin"] = math.sin(2 * math.pi * quarter / 4)
        input_dict["declaration_quarter_cos"] = math.cos(2 * math.pi * quarter / 4)

        severity = input_dict["incident_severity_score"]
        duration = input_dict["incident_duration_days"]
        frequency = input_dict["state_disaster_frequency"]
        days_to_decl = input_dict["days_to_declaration"]

        input_dict["severity_x_duration"] = severity * duration
        input_dict["severity_x_frequency"] = severity * frequency
        input_dict["duration_x_days_to_declaration"] = duration * days_to_decl

        # Build feature vector in the exact order the model expects
        features = [input_dict[col] for col in feature_cols]

        log_pred = float(model.predict([features])[0])
        predicted_cost = float(np.expm1(log_pred))

        response = PredictionResponse(
            predicted_recovery_cost_usd=round(predicted_cost, 2),
            log_prediction=round(log_pred, 4),
            model_type=type(model).__name__,
            timestamp=datetime.utcnow().isoformat(),
        )
        logger.info(f"Prediction successful: ${predicted_cost:,.2f}")
        return response

    except KeyError as e:
        logger.error(f"Missing feature in request: {e}")
        raise HTTPException(status_code=400, detail=f"Missing or mismatched feature: {e}")
    except Exception as e:
        logger.error(f"Prediction failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
def root():
    return {
        "message": "FEMA Disaster Cost Forecaster API",
        "endpoints": ["/health", "/predict-cost", "/docs"],
    }