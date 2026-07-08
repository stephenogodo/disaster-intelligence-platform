# 🌪️ FEMA Disaster Recovery Cost Forecasting Framework

> **End-to-end machine learning system for declaration-time disaster recovery cost prediction**  
> TerraNova Resilience Analytics Ltd · NEXYGENE · July 2026

[![Python](https://img.shields.io/badge/Python-3.13-blue)](https://python.org)
[![XGBoost](https://img.shields.io/badge/XGBoost-3.2.0-orange)](https://xgboost.readthedocs.io)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115.0-green)](https://fastapi.tiangolo.com)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.39.0-red)](https://streamlit.io)
[![Kafka](https://img.shields.io/badge/Kafka-Streaming-black)](https://kafka.apache.org)
[![Docker](https://img.shields.io/badge/Docker-Containerized-blue)](https://docker.com)
[![MLflow](https://img.shields.io/badge/MLflow-Tracked-purple)](https://mlflow.org)
[![Airflow](https://img.shields.io/badge/Airflow-Orchestrated-darkgreen)](https://airflow.apache.org)

---

## Overview

When FEMA declares a major disaster, emergency managers and budget officers must begin planning resource allocation immediately — often days before any reliable cost estimate exists. This system addresses that gap by predicting federal disaster recovery costs **at the point of declaration**, using only information genuinely available at that moment.

The framework integrates three external data sources into a unified feature engineering pipeline, trains an XGBoost regression model tracked via MLflow, and serves predictions through a containerized FastAPI + Streamlit stack. A real-time Kafka streaming layer provides a production ingestion path alongside the batch pipeline, with Airflow DAGs for orchestration.

**Final honest model performance: XGBoost R² = 0.513** — an audited figure that survived correction of a lookahead-bias defect, a severity-score target-leakage issue, and a training/serving scale mismatch, all discovered and fixed during development.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                          INGESTION LAYER                             │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  BATCH PATH  (used by training pipeline)                    │    │
│  │                                                             │    │
│  │  fetch_data_from_API.py                                     │    │
│  │  • Paginates all 3 FEMA endpoints ($skip/$top)              │    │
│  │  • Appends each page incrementally to Parquet               │    │
│  │  • One-shot bulk ingest — no deduplication                  │    │
│  │  • Terminates when API is exhausted                         │    │
│  └───────────────────────────┬─────────────────────────────────┘    │
│                              │                                       │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  STREAMING PATH  (production real-time)                     │    │
│  │                                                             │    │
│  │  kafka_producer.py  →  fema_raw (Kafka topic)              │    │
│  │  • Same 3 endpoints, same pagination logic                  │    │
│  │  • Schema versioning, synchronous delivery confirmation     │    │
│  │  • Graceful SIGINT/SIGTERM shutdown                         │    │
│  │                                                             │    │
│  │  kafka_consumer.py  ←  fema_raw                            │    │
│  │  • Deduplicates by disasterNumber                          │    │
│  │  • Batches 500 records → features.parquet (gzip)           │    │
│  │  • Runs continuously                                        │    │
│  └───────────────────────────┬─────────────────────────────────┘    │
│                              │                                       │
│            declarations / public_assistance / summaries              │
│                         Parquet files                                │
└──────────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│                        TRAINING PIPELINE                             │
│                                                                      │
│  NOAA Storm Events  (FIPS + date join)  ──────────────────────┐     │
│  NOAA GHCN-Daily rainfall (state + date join) ────────────────┤     │
│                                                               ▼     │
│                    clean_and_engineer.py                            │
│                    (1,766 disasters × 62 columns)                   │
│                                                               ▼     │
│                    model_development.py                             │
│                    (RandomizedSearchCV + MLflow)                    │
│                                                               ▼     │
│                    best_model.pkl + feature_columns.pkl             │
└──────────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│                    SERVING STACK  (Docker)                           │
│                                                                      │
│  Streamlit Dashboard  (host :8502)                                   │
│       │  POST /predict-cost                                          │
│       ▼                                                              │
│  FastAPI Backend  (host :8001 / internal :8000)                      │
│       │  derives engineered features server-side                     │
│       │  loads best_model.pkl                                        │
│       ▼                                                              │
│  XGBRegressor  →  predicted_recovery_cost_usd                        │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
FEMA_ML_PROJECT/
│
├── api/                            # FastAPI backend
│   ├── main.py                     # /predict-cost, /health, server-side feature derivation
│   └── __init__.py
│
├── dashboard/                      # Streamlit UI
│   └── app.py                      # Scenario inputs, budget gap chart, API_URL from env
│
├── data_ingestion/                 # Kafka streaming layer (production real-time path)
│   ├── kafka_producer.py           # 3 FEMA endpoints → fema_raw Kafka topic
│   │                               # Schema versioning · delivery confirmation · graceful shutdown
│   └── kafka_consumer.py          # fema_raw → deduplicated Parquet (gzip)
│                                   # In-memory deduplication · batch writes · try/finally save
│
├── direct_data_streaming/          # Batch ingestion + training pipeline
│   ├── src/
│   │   ├── ingestion/
│   │   │   ├── fetch_data_from_API.py      # Batch pull: same 3 FEMA endpoints,
│   │   │   │                               # incremental Parquet append, no deduplication.
│   │   │   │                               # Used by the training pipeline.
│   │   │   ├── climate_ingest.py           # NOAA Storm Events (wind_speed, flood_severity)
│   │   │   │                               # 31yr download · county FIPS + date join
│   │   │   └── rainfall_data_ingestion.py  # NOAA GHCN-Daily (rainfall_intensity)
│   │   │                                   # 31yr download · one-year-at-a-time · state join
│   │   └── processing/
│   │       ├── clean_and_engineer.py       # Master feature engineering:
│   │       │                               # merge sources, severity score, interactions,
│   │       │                               # cyclical encoding, lookahead-bias-corrected freq
│   │       ├── EDA.py                      # Exploratory data analysis + 6 charts
│   │       └── compute_disaster_risk_score.py
│   ├── data/                       # raw/ and processed/ — gitignored
│   └── reports/eda/                # EDA charts (PNG) + outliers.csv
│
├── ml/                             # Model development
│   ├── model_development.py        # LR / RF / XGBoost · RandomizedSearchCV · MLflow
│   ├── inspect_feature_importance.py        # MDI importance
│   ├── permutation_feature_importance_check.py  # Bias-corrected importance
│   └── logs/
│
├── airflow/                        # Pipeline orchestration
│   └── dags/fema_pipelin.py        # Full pipeline DAG (note: filename typo, flagged)
│
├── models/
│   ├── best_model.pkl              # gitignored — regenerate (see Quickstart)
│   ├── feature_columns.pkl         # committed — 23-feature ordered list
│   └── xgboost_fema_model.pkl      # gitignored
│
├── infrastructure/
│   └── aws_setup.md
│
├── reports/
│   ├── feature_importance.csv
│   └── permutation_importance.csv
│
├── Dockerfile.api
├── Dockerfile.dashboard
├── docker-compose.yaml
├── requirements-api.txt
├── requirements-dashboard.txt
├── requirements.txt
├── .env.example                    # Copy to .env and configure
└── FEMA_ML_Portfolio_Documentation_v2.docx
```

---

## Ingestion Paths — Batch vs. Streaming

The project implements two parallel ingestion paths for the same three FEMA endpoints. They are complementary, not redundant.

| Feature | `fetch_data_from_API.py` | Kafka pipeline |
|---------|--------------------------|----------------|
| Same 3 FEMA endpoints | ✅ | ✅ |
| Incremental Parquet append | ✅ | ✅ |
| Deduplication | ❌ | ✅ |
| Continuous operation | ❌ (terminates) | ✅ |
| Graceful SIGINT/SIGTERM shutdown | ❌ | ✅ |
| Schema versioning on events | ❌ | ✅ |
| Broker-mediated decoupling | ❌ (direct HTTP→disk) | ✅ |
| Environment variable config | ❌ (hardcoded paths) | ✅ |
| **Used by training pipeline** | **✅** | ❌ |

---

## Data Sources

| Source | Records | Join Key | Signals |
|--------|---------|----------|---------|
| FEMA DisasterDeclarationsSummaries | 209,310 raw → 1,766 training | disasterNumber | incident type, dates, state, region |
| FEMA PublicAssistanceFundedProjectsDetails | 810,774 projects | disasterNumber | total_obligated_amount (target) |
| NOAA Storm Events (1996–2026) | 1,059,514 county-level events | FIPS code + date window | wind_speed, flood_severity |
| NOAA GHCN-Daily (1996–2026) | ~187M US PRCP rows | state + date window | rainfall_intensity |

**Climate match rates (against 1,766 training disasters):**  
Storm Events: 72.0% · GHCN-Daily: 95.8%

---

## Features (23 model inputs)

| Category | Features |
|----------|---------|
| Temporal | `declaration_year`, `declaration_month_sin/cos`, `declaration_quarter_sin/cos` |
| Duration & Lag | `incident_duration_days`, `days_to_declaration` |
| Geographic | `region_encoded` |
| Severity | `incident_severity_score` (60% incident-type + 40% NOAA climate blend) |
| Disaster History | `state_disaster_frequency` (cumulative count — bias-corrected) |
| Incident Type Flags | `is_hurricane`, `is_flood`, `is_fire`, `is_severe_storm`, `is_tornado`, `is_snowstorm`, `is_biological`, `is_severe_ice_storm`, `is_typhoon`, `is_drought` |
| Interactions | `severity_x_duration`, `severity_x_frequency`, `duration_x_days_to_declaration` |

### Severity Score (Final Formula)

```
incident_severity_score = 0.60 × type_severity_normalized
                        + 0.40 × mean(normalize(wind_speed),
                                      normalize(flood_severity),
                                      normalize(rainfall_intensity))
```

`type_severity_normalized`: fixed 0–1 lookup — Hurricane/Typhoon=1.0, Flood/Tornado=0.75, Severe Storm/Fire=0.50, Drought/Snowstorm=0.25, Other=0.0.

The severity score went through four development stages — see portfolio documentation for the full target-leakage, scale-mismatch, and redundancy investigation.

---

## Model Results

| Model | CV R² | Test R² | Test RMSE | Test MAE |
|-------|--------|---------|-----------|---------|
| Linear Regression | — | 0.234 | 1.874 | 1.308 |
| Random Forest | 0.396 | 0.466 | 1.565 | 1.146 |
| **XGBoost (best)** | **0.386** | **0.513** | **1.494** | **1.092** |

> **On the R² figure:** earlier runs showed 0.591–0.598. The drop to 0.513 reflects correction of a lookahead bias in `state_disaster_frequency` that removed ~0.08 R² of illegitimate future-information leakage. The 0.513 is the correct, defensible figure.

### Permutation Feature Importance (final model)

| Rank | Feature | Importance |
|------|---------|-----------|
| 1 | days_to_declaration | 0.1826 |
| 2 | declaration_year | 0.1309 |
| 3 | severity_x_duration | 0.0916 |
| 4 | incident_duration_days | 0.0826 |
| 5 | incident_severity_score | 0.0712 |
| 6 | duration_x_days_to_declaration | 0.0677 |
| 7 | state_disaster_frequency | 0.0531 |

---

## Quickstart

### Prerequisites
- Python 3.11+, Docker Desktop, Kafka (streaming path only)

### 1. Clone and configure
```bash
git clone https://github.com/YOUR_USERNAME/FEMA_ML_PROJECT.git
cd FEMA_ML_PROJECT
cp .env.example .env
python -m venv .venv && .venv\Scripts\Activate.ps1   # Windows
pip install -r requirements.txt
```

### 2. Run the training pipeline (batch path)
```bash
# Ingest FEMA data
python -m direct_data_streaming.src.ingestion.fetch_data_from_API

# Ingest NOAA climate data (~5GB download, cached after first run)
python -m direct_data_streaming.src.ingestion.climate_ingest
python -m direct_data_streaming.src.ingestion.rainfall_data_ingestion

# Engineer features
python -m direct_data_streaming.src.processing.clean_and_engineer

# Train models (MLflow-tracked)
python -m ml.model_development
```

### 3. Run the serving stack (Docker)
```bash
$env:DOCKER_BUILDKIT=0   # Windows — avoids BuildKit/WSL2 path issue
docker-compose up --build
```
- API docs: http://localhost:8001/docs  
- Dashboard: http://localhost:8502

### 4. Run locally (without Docker)
```bash
# Terminal 1 — API
uvicorn api.main:app --reload --port 8000

# Terminal 2 — Dashboard
streamlit run dashboard/app.py
```

### 5. Run the Kafka streaming layer
```bash
# Requires a Kafka broker on localhost:9092 (or set KAFKA_BOOTSTRAP in .env)

# Terminal 1 — Producer
python data_ingestion/kafka_producer.py

# Terminal 2 — Consumer
python data_ingestion/kafka_consumer.py
```

### 6. View MLflow experiment runs
```bash
mlflow ui --backend-store-uri sqlite:///mlflow.db
# Open: http://localhost:5000
```

---

## API Reference

### `POST /predict-cost`

```json
{
  "region_encoded": 4,
  "incident_duration_days": 14,
  "days_to_declaration": 5,
  "state_disaster_frequency": 10,
  "incident_severity_score": 1.0,
  "declaration_year": 2026,
  "declaration_month": 9,
  "declaration_quarter": 3,
  "is_hurricane": 1,
  "is_flood": 0,
  "is_fire": 0,
  "is_severe_storm": 0,
  "is_tornado": 0,
  "is_snowstorm": 0,
  "is_severe_ice_storm": 0,
  "is_typhoon": 0,
  "is_drought": 0,
  "is_biological": 0
}
```

**Response:**
```json
{
  "predicted_recovery_cost_usd": 60392191.53,
  "log_prediction": 17.9164,
  "model_type": "XGBRegressor",
  "timestamp": "2026-07-06T21:04:38.989549"
}
```

> All engineered features (`declaration_month_sin/cos`, `severity_x_duration`, etc.) are derived server-side from the 18 raw inputs — callers never compute them.

### `GET /health`
```json
{"status": "ok", "model_loaded": true}
```

---

## Validated Severity Ladder

Duration=14 days, days_to_declaration=5, state_disaster_frequency=10 — all other params fixed:

| Incident Type | Severity | Predicted Cost |
|--------------|---------|---------------|
| Other (baseline) | 0.00 | $7,004,323 |
| Drought | 0.25 | $16,906,769 |
| Severe Storm | 0.50 | $25,981,839 |
| Flood | 0.75 | $46,137,326 |
| Hurricane | 1.00 | $60,392,192 |

Strictly monotonic — no reversals across the full 0–1 severity range.

---

## Known Limitations

| Limitation | Detail |
|-----------|--------|
| Small training set | n=1,766; CV/test gap (~0.10 R²); test score may be optimistic |
| Batch path lacks deduplication | Running `fetch_data_from_API.py` twice produces duplicate rows — use Kafka path for repeated ingestion |
| Rainfall join is state-level | Lower spatial precision than Storm Events' county FIPS join |
| No prediction intervals | Budget gap chart uses fixed % multipliers, not statistical confidence bounds |
| Calendar year at rank #2 | May encode inflation/programme growth rather than genuine risk drivers |
| No runtime severity validation | API accepts inconsistent inputs (severity=0.0 with is_hurricane=1) |
| kafka_consumer memory growth | `processed_ids` set grows unboundedly in long-running deployments |

---

## Technical Stack

| Category | Technology |
|----------|-----------|
| Language | Python 3.13 |
| ML | XGBoost 3.2.0, scikit-learn 1.5.2 |
| Experiment Tracking | MLflow (SQLite backend) |
| API | FastAPI 0.115.0 + Uvicorn 0.32.0 |
| Dashboard | Streamlit 1.39.0 + Plotly 5.24.1 |
| Streaming | Apache Kafka (kafka-python) |
| Orchestration | Apache Airflow |
| Containerization | Docker + docker-compose |
| Data Storage | Parquet (PyArrow), SQLite |
| Data Sources | FEMA Open API, NOAA NCDC Storm Events, NOAA NCEI GHCN-Daily |

---

## License

See [LICENSE](LICENSE) for details.

---

*FEMA Disaster Recovery Cost Forecasting Framework · TerraNova Resilience Analytics Ltd · NEXYGENE · July 2026*
