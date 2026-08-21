# FEMA Disaster Recovery Cost Forecasting Framework
**TerraNova Resilience Analytics Ltd**
*Climate Risk & Public Sector Analytics*

---

================================================================================
## 1. Project Purpose
================================================================================

### The Problem TerraNova Is Solving

When a major disaster is declared, emergency managers, public-sector budget officers, and other decision-makers must begin allocating resources almost immediately. Yet at that stage, the eventual cost of disaster recovery is highly uncertain.

Traditional recovery-cost information becomes clearer only as the disaster progresses and additional damage, assistance, and expenditure information becomes available. This creates an important planning gap:

> **How can decision-makers estimate the likely scale of disaster recovery costs early enough to inform resource allocation and financial planning?**

TerraNova Resilience Analytics Ltd developed this platform to address that gap.

The platform uses historical FEMA disaster declarations, FEMA Public Assistance information, and NOAA climate and event-intensity information to develop a machine-learning framework for forecasting disaster recovery costs.

The objective is not to produce a precise final expenditure figure at the moment of declaration. Instead, the platform provides a data-driven estimate of the likely recovery-cost scale that can support:

- early budgeting;
- resource allocation;
- funding-gap analysis;
- scenario planning; and
- disaster recovery preparedness.

### Why Early Forecasting Matters

A disaster recovery forecast has different information requirements depending on when the forecast is produced.

Immediately after or during a disaster declaration, important information such as final incident duration may not yet be known. A forecasting system that depends on information that only becomes available later cannot support the earliest decision-making window.

The platform therefore provides two forecasting models designed for different information stages.

### Model B — Early Forecast

Model B is designed for early forecasting.

It does **not** require incident duration and uses information that can be available during the early stage of a disaster.

This makes Model B appropriate when decision-makers need an initial estimate before the complete characteristics of the incident are known.

### Model A — Standard Forecast

Model A is designed for a more complete scenario forecast.

It uses incident duration and duration-dependent engineered features when that information is available.

Model A therefore represents the more information-rich forecasting scenario.

### Two-Stage Forecasting Concept

```text
Early disaster stage
        |
        v
   Model B
Early Forecast
(no duration required)
        |
        |  More information becomes available
        v
   Model A
Standard Forecast
(duration available)

The two models are complementary rather than competing implementations.

================================================================================
## 2. Project Objectives
================================================================================

The platform is designed to:

integrate FEMA disaster declaration information;
integrate FEMA Public Assistance information;
incorporate NOAA climate and event-intensity information;
engineer consistent predictive features;
support early recovery-cost forecasting;
support standard scenario forecasting;
provide model-selection flexibility between Model A and Model B;
provide budget-gap scenario analysis;
expose forecasts through a FastAPI service;
provide an interactive Streamlit dashboard;
support reproducible local development;
support Dockerized model serving; and
establish a foundation for future production-scale deployment.
================================================================================
## 3. Current Architecture
================================================================================

The current platform separates data ingestion, feature engineering, forecasting, and serving.

                 FEMA / NOAA DATA SOURCES
                           |
                           v
                 +---------------------+
                 |   Data Ingestion    |
                 +----------+----------+
                            |
                  +---------+---------+
                  |                   |
             Batch path          Kafka path
              PRIMARY           EXPERIMENTAL
                  |                   |
                  +---------+---------+
                            |
                            v
              +-----------------------+
              | Common Feature        |
              | Engineering           |
              | clean_and_engineer.py |
              +-----------+-----------+
                          |
                  +-------+-------+
                  |               |
              Model A         Model B
             Standard           Early
              Forecast         Forecast
                  |               |
                  +-------+-------+
                          |
                          v
                +-------------------+
                |    FastAPI API    |
                +---------+---------+
                          |
                          v
                +-------------------+
                | Streamlit         |
                | Dashboard         |
                +-------------------+

The current containerized serving architecture consists of:

FastAPI API;
Streamlit dashboard; and
Docker Compose.

Kafka is deliberately not part of the normal Docker Compose startup.

Airflow and workflow orchestration are not part of the current architecture.

================================================================================
## 4. Core Architectural Principle
================================================================================
Ingestion Type Is Independent of Model Choice

The platform deliberately separates ingestion from forecasting.

There are currently two ingestion approaches:

Batch ingestion — primary path
Kafka ingestion — optional experimental path

Both ultimately feed the same common feature-engineering layer.

                    INGESTION
                        |
              +---------+---------+
              |                   |
            Batch               Kafka
           PRIMARY           EXPERIMENTAL
              |                   |
              +---------+---------+
                        |
                        v
             clean_and_engineer.py
                        |
                 Common features
                        |
              +---------+---------+
              |                   |
           Model A             Model B
         Standard              Early
         Forecast             Forecast

The ingestion mechanism does not determine the forecasting model.

This separation is intentional.

It allows TerraNova to change the data-ingestion architecture without redesigning the forecasting layer.

================================================================================
## 5. Data Sources
================================================================================

The platform works with disaster and environmental information from public-sector sources.

Primary source categories include:

FEMA disaster declarations;
FEMA Public Assistance data;
NOAA Storm Events data;
NOAA GHCN-Daily data;
rainfall information; and
climate/event-intensity information.

The raw data are transformed into analytical datasets and then into engineered model features.

================================================================================
## 6. Data Ingestion
================================================================================
### 6.1 Batch Ingestion — Primary Path

The primary ingestion implementation is located under:

data_ingestion/batch/

The batch layer contains components for:

FEMA data ingestion;
climate ingestion;
rainfall ingestion;
incremental data processing; and
rainfall matching.

The batch path is the recommended ingestion mechanism for normal development and operation.

It is intentionally simpler than the experimental streaming architecture and is currently the most practical approach for the project's data volume and update requirements.

### 6.2 Common Ingestion Utilities

Shared ingestion functionality is located under:

data_ingestion/common/

Important components include:

data_source.py
incremental_update.py

These components provide reusable functionality for data sources and incremental updates.

### 6.3 Kafka Ingestion — Optional Experimental Path

The Kafka implementation is located under:

data_ingestion/kafka/

Kafka is retained as an experimental alternative to the primary batch ingestion mechanism.

It is not required for:

model training;
model inference;
Model A;
Model B;
FastAPI;
Streamlit; or
the normal Docker Compose deployment.
Why Kafka Is Currently Experimental

The current Kafka-to-Parquet workflow can repeatedly perform a cycle similar to:

Read existing Parquet
        |
Append new batch
        |
Deduplicate
        |
Rewrite Parquet
        |
Read again for next batch
        |
       ...

This makes the current implementation inefficient as the historical dataset grows.

The project therefore deliberately avoids treating Kafka as the primary production ingestion mechanism at this stage.

The time and engineering effort required to optimize the current implementation were not justified relative to the value obtained from the streaming path during the current project phase.

Kafka remains valuable as an experimental foundation for future optimization.

================================================================================
## 7. Common Feature Engineering
================================================================================

Both ingestion paths ultimately feed the same feature-engineering layer:

feature_engineering/clean_and_engineer.py

This creates an important architectural boundary.

Batch ------------------+
                         |
                         v
              clean_and_engineer.py
                         |
                         +----> Model A
                         |
                         +----> Model B
                         ^
                         |
Kafka ------------------+

The feature-engineering layer provides the common representation used by the forecasting models.

This prevents the ingestion mechanism from creating separate and potentially inconsistent feature definitions.

================================================================================
## 8. Forecasting Models
================================================================================
### 8.1 Model A — Standard Forecast

Model A is the standard forecasting model.

It requires:

incident_duration_days

and uses duration-dependent engineered features.

Model A is appropriate when the incident duration is known or when the forecasting scenario intentionally includes an estimated duration.

Model A feature concept

Model A uses features including:

FEMA region;
incident duration;
days to declaration;
state disaster frequency;
incident severity score;
declaration year;
declaration month transformations;
declaration quarter transformations;
severity/frequency interactions;
severity/duration interactions;
duration/declaration interactions; and
incident-type indicators.

The exact model feature list is maintained with the trained model artifacts.

### 8.2 Model B — Early Forecast

Model B is designed specifically for early forecasting.

It does not require:

incident_duration_days

It also does not use duration-dependent engineered features.

The API performs a Model B safety check to ensure that duration-dependent features are not included.

This is an important architectural and methodological constraint.

Model B feature concept

Model B uses information such as:

FEMA region;
days to declaration;
state disaster frequency;
incident severity score;
declaration year;
declaration month transformations;
declaration quarter transformations;
severity/frequency interaction; and
incident-type indicators.
================================================================================
## 9. Model Selection
================================================================================

Model selection is independent of ingestion selection.

The system therefore supports combinations such as:

Batch + Model A
Batch + Model B
Kafka + Model A
Kafka + Model B

Although Kafka is experimental, the architecture does not artificially couple it to either forecasting model.

The forecasting decision is based on information availability, not on how the information was ingested.

================================================================================
## 10. Forecasting Workflow
================================================================================

The standard analytical flow is:

Data Sources
     |
     v
Data Ingestion
     |
     v
Data Cleaning
     |
     v
Feature Engineering
     |
     v
Model Selection
     |
     +------------------+
     |                  |
   Model A            Model B
     |                  |
     +--------+---------+
              |
              v
       Recovery Cost
          Forecast
              |
              v
       Budget Analysis
================================================================================
## 11. Model Development and Evaluation
================================================================================

The repository contains a dedicated ML development layer under:

ml/

It contains functionality for:

model development;
early-forecast model development;
temporal validation;
Model B temporal validation;
current-model evaluation;
feature-importance inspection;
permutation feature importance; and
regime comparison.

Important development and validation work included addressing methodological risks such as:

temporal/lookahead leakage;
severity-score target leakage; and
training-versus-serving feature-scale inconsistencies.

These issues were identified and addressed as part of the model-development process.

The project's model results should therefore be interpreted in the context of the historical data used for training and validation.

================================================================================
## 12. Model Validation and Performance
================================================================================

Model performance was evaluated using both a conventional random holdout and a chronological temporal validation strategy.

### 12.1 Random Holdout Evaluation

The existing production Model A artifact was evaluated using an 80/20 random train/test split.

| Metric | Result |
|---|---:|
| Test samples | 312 |
| Log-space R² | 0.4278 |
| Log-space RMSE | 1.6841 |
| Log-space MAE | 1.1082 |
| Dollar-space MAE | $338.21M |
| Dollar-space RMSE | $2.61B |
| Actual median | $7.24M |
| Predicted median | $8.10M |

Because disaster recovery costs are highly skewed, dollar-space metrics are strongly influenced by a small number of very large disasters. The random holdout should therefore not be interpreted as the primary estimate of future forecasting performance.

### 12.2 Temporal Validation

A chronological validation was performed to better represent the real forecasting problem: models were trained on declarations from **1998–2022** and evaluated on later declarations from **2023–2026**.

The temporal test contained 88 observations.

| Metric | Model A — Standard Forecast | Model B — Early Forecast |
|---|---:|---:|
| Training period | 1998–2022 | 1998–2022 |
| Test period | 2023–2026 | 2023–2026 |
| Training samples | 1,468 | 1,468 |
| Test samples | 88 | 88 |
| Features | 23 | 20 |
| R² | 0.1201 | **0.1353** |
| RMSE | 2.2413 | **2.2218** |
| MAE | **1.6802** | 1.6906 |

Model B slightly outperformed Model A on temporal R² and RMSE despite using fewer features and excluding incident duration. This supports maintaining two complementary forecasting modes rather than treating Model B as simply a reduced version of Model A.

The temporal results also demonstrate an important limitation: future disaster recovery costs remain difficult to predict from declaration-time information alone. The models should therefore be interpreted as **decision-support and budgeting estimates**, rather than precise point forecasts.

The temporal validation scripts train temporary evaluation models and do not modify the production model artifacts.

### 12.3 Interpretation

The validation results should be read alongside the project's known limitations:

- The target is highly skewed, with a small number of exceptionally costly disasters.
- Random holdout performance is more optimistic than chronological future-period performance.
- Temporal validation is the more relevant measure for assessing expected future forecasting behavior.
- Model B achieves comparable, and slightly better, temporal performance than Model A while requiring less information.
- The forecasts are intended to support budgeting, scenario analysis, and resource-allocation decisions rather than replace formal financial estimation.

================================================================================
## 13. Model Artifacts
================================================================================

The trained model artifacts are stored under:

models/

Current model artifacts include:

best_model.pkl
feature_columns.pkl
model_b_early_forecast.pkl
model_b_feature_columns.pkl
xgboost_fema_model.pkl

These artifacts are used by the API serving layer.

The models are treated as runtime artifacts rather than source-code replacements for the model-development process.

================================================================================
## 14. FastAPI Serving Layer
================================================================================

The API is implemented under:

api/main.py

The API loads both Model A and Model B.

It exposes health information and prediction functionality.

Health Endpoint
GET /health

The validated API health response reports information equivalent to:

{
  "status": "ok",
  "model_a_loaded": true,
  "model_b_loaded": true,
  "default_model_version": "B",
  "available_models": ["A", "B"]
}

This confirms that both forecasting models are available to the serving application.

================================================================================
## 15. Model Version Selection Through the API
================================================================================

The API supports explicit model selection through:

X-Model-Version: A

or:

X-Model-Version: B
Model B

Model B accepts an early-forecast scenario without incident duration.

Example conceptual inputs include:

region_encoded
days_to_declaration
state_disaster_frequency
incident_severity_score
declaration_year
declaration_month
declaration_quarter
incident-type indicators
Model A

Model A accepts the standard scenario and includes:

incident_duration_days

along with its duration-dependent features.

================================================================================
## 16. Example API Forecasts
================================================================================

The Dockerized API was validated using both models.

Model B — Early Forecast

A validated scenario using:

Region: 4
Days to declaration: 7
State disaster frequency: 12
Incident severity: 0.62
Hurricane: 1
Flood: 1

returned a recovery-cost estimate of approximately:

$1,995,962.75

under Model B.

Model A — Standard Forecast

Using the same scenario with:

Incident duration: 14 days

Model A returned a recovery-cost estimate of approximately:

$4,512,103.39

These are example scenario outputs, not guarantees of actual disaster recovery expenditure.

================================================================================
## 17. Streamlit Dashboard
================================================================================

The dashboard is implemented under:

dashboard/app.py

It provides an interactive scenario-based forecasting interface.

The interface supports:

forecasting model selection;
incident type selection;
FEMA region;
incident duration when Model A is selected;
days to declaration;
state disaster frequency;
incident severity score; and
declaration date.

The dashboard also provides:

predicted recovery cost;
forecast type;
forecast mode;
budget-gap analysis; and
scenario summary.
================================================================================
## 18. Dashboard Forecast Modes
================================================================================
Model B — Early Forecast

When Model B is selected, incident duration is not required.

The dashboard communicates this explicitly:

Model B forecasts without incident duration.

This reflects the intended early-warning use case.

Model A — Standard Forecast

When Model A is selected, incident duration becomes part of the scenario.

The dashboard communicates that Model A uses incident duration for standard forecasting.

================================================================================
## 19. Budget Gap Analysis
================================================================================

The dashboard includes budget-gap analysis to help users understand how different budget allocation scenarios compare with the predicted recovery cost.

The forecast should therefore be interpreted not simply as a single number, but as a planning input.

A decision-maker can use the estimated recovery cost to consider:

potential funding requirements;
budget shortfalls;
contingency allocations; and
alternative allocation scenarios.
================================================================================
## 20. Docker Deployment
================================================================================

Docker Desktop is the recommended containerized serving environment for the current project.

The current Compose deployment contains two services:

api
dashboard

Kafka is intentionally excluded from the default deployment.

================================================================================
## 21. Docker API
================================================================================

The API is built using:

Dockerfile.api

The container:

uses Python 3.11;
installs API-specific dependencies;
copies the API source;
copies trained model artifacts;
exposes container port 8000; and
runs Uvicorn.

Host mapping:

localhost:8001 -> container:8000
================================================================================
## 22. Docker Dashboard
================================================================================

The dashboard is built using:

Dockerfile.dashboard

The container:

uses Python 3.11;
installs dashboard-specific dependencies;
copies the dashboard source;
exposes container port 8501; and
runs Streamlit.

Host mapping:

localhost:8502 -> container:8501
================================================================================
## 23. Docker Compose
================================================================================

The platform is started with:

docker compose up -d

Check the services with:

docker compose ps

Expected services:

fema-cost-api
fema-cost-dashboard

Both services should report healthy status after startup.

================================================================================
## 24. Docker Commands
================================================================================
Build
docker compose build
Start
docker compose up -d
Check status
docker compose ps
Stop
docker compose down
View logs
docker compose logs
View API logs
docker compose logs api
View dashboard logs
docker compose logs dashboard
================================================================================
## 25. Docker Access Points
================================================================================

Once the containers are running:

API
http://127.0.0.1:8001
API documentation
http://127.0.0.1:8001/docs
API health
http://127.0.0.1:8001/health
Streamlit dashboard
http://127.0.0.1:8502

In PowerShell, entering a URL by itself is interpreted as a command. Open the dashboard URL in a browser rather than typing the URL directly into PowerShell.

================================================================================
## 26. Docker Networking
================================================================================

Inside the Docker Compose network, the dashboard communicates with the API using:

http://api:8000

The dashboard should not use:

http://localhost:8001

for container-to-container communication.

The distinction is:

Host machine
    |
    +--> localhost:8001 --> API container :8000
    |
    +--> localhost:8502 --> Dashboard container :8501


Dashboard container
    |
    +--> http://api:8000 --> API container
================================================================================
## 27. Docker Build Context
================================================================================

The project uses:

.dockerignore

to keep unnecessary files out of Docker build contexts.

Excluded material includes categories such as:

Python virtual environments;
Python caches;
logs;
MLflow runtime data;
reports where not required by the serving image;
Git metadata; and
other development-only artifacts.

The API and dashboard Dockerfiles also copy only the material required for their respective runtime responsibilities.

================================================================================
## 28. Local Development
================================================================================

The project retains:

run_platform.py

as the local-development launcher.

Run:

python .\run_platform.py

The local launcher provides the mechanism for selecting the appropriate platform operation while keeping ingestion and forecasting decisions separate.

For normal development, use the batch ingestion path.

Kafka should be used only when explicitly testing the experimental streaming implementation.

================================================================================
## 29. Local Versus Docker Operation
================================================================================

The project intentionally distinguishes local development from containerized serving.

Local development
run_platform.py
      |
      +---- Batch ingestion
      |          |
      |          v
      |   Feature Engineering
      |          |
      |          v
      |       Model A/B
      |
      +---- Optional Kafka experiment
Containerized serving
Docker Compose
      |
      +---- FastAPI
      |       |
      |     Model A/B
      |
      +---- Streamlit
              |
          API requests

Docker is the preferred current serving/deployment environment.

================================================================================
## 30. Repository Structure
================================================================================

The current project structure is approximately:

disaster-intelligence-platform/
│   ├── batch/
│   │   ├── __init__.py
│   │   ├── batch_data_ingestor.py
│   │   ├── climate_ingest.py
│   │   ├── rainfall_data_ingestion.py
│   │   └── rainfall_pct_match.py
│   │
│   ├── common/
│   │   ├── data_source.py
│   │   └── incremental_update.py
│   │
│   └── kafka/
│       ├── __init__.py
│       ├── cursor_manager.py
│       ├── kafka_consumer.py
│       ├── kafka_producer.py
│       └── kafka_smoke_test.py
│
├── feature_engineering/
│   └── clean_and_engineer.py
│
├── ml/
│   ├── __init__.py
│   ├── compare_regimes.py
│   ├── evaluate_current_model.py
│   ├── inspect_feature_importance.py
│   ├── model_b_early_forecast.py
│   ├── model_development.py
│   ├── permutation_feature_importance_check.py
│   ├── temporal_validation.py
│   └── temporal_validation_model_b.py
│
├── models/
│   ├── best_model.pkl
│   ├── feature_columns.pkl
│   ├── model_b_early_forecast.pkl
│   ├── model_b_feature_columns.pkl
│   └── xgboost_fema_model.pkl
│
├── storage/
│   ├── __init__.py
│   ├── checkpoint_manager.py
│   ├── config.py
│   ├── download_state.py
│   ├── json_store.py
│   └── parquet_repository.py
│
├── data/
│   ├── metadata/
│   ├── processed/
│   └── raw/
│
├── docs/
├── notebooks/
├── reports/
├── scripts/
├── tests/
│
├── run_platform.py
│
├── Dockerfile.api
├── Dockerfile.dashboard
├── docker-compose.yaml
├── .dockerignore
├── .gitignore
│
├── requirements.txt
├── requirements-api.txt
├── requirements-dashboard.txt
├── README.md
└── LICENSE

The repository also contains legacy/experimental material that is not part of the current operational architecture.

In particular, workflow orchestration is not a current platform component.

================================================================================
## 31. Storage Architecture
================================================================================

The current platform uses Parquet for analytical data storage.

Important storage components include:

storage/parquet_repository.py
storage/checkpoint_manager.py
storage/download_state.py
storage/config.py
storage/json_store.py

The storage layer supports incremental data processing and checkpoint/state management.

The current architecture intentionally favors a straightforward analytical storage approach while the project remains at its current scale.

================================================================================
## 32. Data Organization
================================================================================

The main data directories are:

data/raw/
data/processed/
data/metadata/

Raw datasets contain source-level information.

Processed datasets contain transformed analytical data.

Metadata and checkpoint files support incremental ingestion and processing state.

Large generated datasets are not intended to become part of ordinary source-code commits.

================================================================================
## 33. Configuration and Environment
================================================================================

Environment-specific configuration should be kept outside source control.

The repository uses:

.env

for local environment configuration where required.

The .gitignore excludes environment files and other sensitive or machine-specific artifacts.

For Docker Compose, the dashboard receives the API endpoint through:

API_URL=http://api:8000
================================================================================
## 34. Testing and Validation
================================================================================

The repository contains tests and diagnostic scripts covering areas including:

FEMA data queries;
FEMA Public Assistance queries;
storage;
downloads; and
model-related validation.

A basic Python syntax check can be performed with:

python -m py_compile .\run_platform.py

The Docker API can be checked with:

Invoke-RestMethod http://127.0.0.1:8001/health

A healthy deployment should report:

status                : ok
model_a_loaded        : True
model_b_loaded        : True
default_model_version : B
available_models      : {A, B}
================================================================================
## 35. Current Validation Status
================================================================================

The containerized serving architecture has been successfully validated through clean startup and restart cycles.

Validation included:

successful Docker image builds;
successful API container startup;
successful dashboard container startup;
API health checks;
successful Model A loading;
successful Model B loading;
successful Model B duration-independence safety check;
successful Model B API prediction;
successful Model A API prediction;
successful Streamlit Model B scenario execution;
successful Streamlit Model A scenario execution;
clean container shutdown; and
clean subsequent container startup.

The validated port mappings are:

API:
host 8001 -> container 8000


Dashboard:
host 8502 -> container 8501
================================================================================
## 36. Known Limitations
================================================================================
### 36.1 Historical Data Limitations

The models depend on historical disaster and assistance information.

Historical patterns cannot fully represent every future disaster.

Extreme events may differ substantially from historical observations.

### 36.2 Forecast Uncertainty

A predicted recovery cost is an estimate.

It should not be interpreted as a guaranteed final expenditure.

The dashboard is intended to support scenario planning and decision support rather than replace formal financial estimation processes.

### 36.3 Severity Representation

The incident severity score provides a structured representation of disaster severity.

It may not capture every dimension of real-world disaster intensity.

Additional event-specific intelligence, such as high wind speeds or major flooding indicators, can potentially improve severity representation.

### 36.4 Geographic Resolution

Climate, rainfall, disaster declarations, and assistance records can have different spatial resolutions.

Consequently, localized disaster impacts may not always be perfectly represented by the available source data.

### 36.5 Kafka Performance

The current experimental Kafka path can repeatedly read and rewrite historical Parquet data.

This makes it unsuitable as the preferred high-frequency streaming architecture in its current form.

This is a known limitation and a defined future-work item.

### 36.6 Model Serialization Compatibility

The current API successfully loads the serialized XGBoost model artifacts, but the runtime reports a warning concerning loading serialized models created under an older XGBoost environment.

The models currently load and serve predictions successfully.

For future productionization, model artifacts should be regenerated and versioned under a controlled and explicitly documented runtime environment.

================================================================================
## 37. Architectural Decisions
================================================================================

The following decisions are intentional.

Batch is the primary ingestion path

Batch ingestion provides the most practical and time-efficient approach for the current project.

Kafka is optional

Kafka is retained as an experimental alternative rather than a mandatory production dependency.

Kafka is not part of normal Docker startup

The default Docker Compose deployment starts only the API and dashboard.

Feature engineering is shared

Both ingestion paths feed:

feature_engineering/clean_and_engineer.py
Model choice is independent of ingestion

The system does not force Model A or Model B based on the ingestion mechanism.

Model B remains duration-independent

Model B must not use incident duration or duration-dependent features.

Model A uses incident duration

Model A is the standard scenario model and uses incident duration.

Docker is the current serving mechanism

The FastAPI and Streamlit services are containerized for reproducible serving.

Local development remains available

run_platform.py remains the local-development entry point.

Orchestration is not currently part of the platform

Airflow and DAG orchestration were intentionally discontinued from the current implementation and are reserved for future development.

================================================================================
## 38. Future Work
================================================================================

The current platform establishes a functional forecasting and serving baseline.

The following capabilities remain part of the longer-term roadmap.

### 38.1 More Efficient Kafka Streaming

The Kafka ingestion path should be redesigned to eliminate the current repeated:

read -> append -> deduplicate -> rewrite

cycle across the full historical Parquet dataset.

Potential future approaches include:

partitioned datasets;
append-oriented storage;
incremental deduplication state;
event-based processing;
optimized checkpointing;
a streaming-native storage layer; and
more efficient batch compaction.

Kafka can then be reassessed as a production ingestion option.

### 38.2 Airflow and DAG Orchestration

Workflow orchestration can be reintroduced when the underlying ingestion and processing components are sufficiently stable.

Potential DAGs could coordinate:

Data ingestion
      |
      v
Validation
      |
      v
Feature engineering
      |
      v
Model training / refresh
      |
      v
Evaluation
      |
      v
Artifact management

Potential Airflow workflows could support:

scheduled FEMA ingestion;
scheduled climate ingestion;
data-quality validation;
feature-engineering jobs;
model retraining;
model evaluation; and
operational monitoring.

Airflow is therefore a future capability, not a current dependency.

### 38.3 CI/CD

A production-grade CI/CD pipeline should be developed to automate:

unit tests;
integration tests;
Python quality checks;
API validation;
model-artifact validation;
Docker image builds;
dependency security checks;
container security checks;
versioning;
artifact management; and
controlled deployment.

The goal is to move from manually validated builds toward repeatable automated software delivery.

### 38.4 Cloud Deployment

The Dockerized serving stack should eventually be deployed to cloud infrastructure.

Future cloud deployment should address:

container hosting;
scalable API serving;
dashboard hosting;
durable data storage;
model artifact management;
secrets management;
monitoring;
logging;
networking; and
automated deployment.

The existing containerized architecture provides a natural foundation for this transition.

### 38.5 Production-Scale Data Architecture

As data volume and ingestion frequency increase, the current storage architecture should be reassessed.

Future architecture may separate:

Low-latency ingestion/state
            |
            v
Streaming / operational storage
            |
            v
Historical analytical storage
            |
            v
Feature engineering
            |
            v
Model serving

This would prevent high-frequency ingestion workloads from repeatedly rewriting large historical analytical datasets.

### 38.6 Model Improvements

Future model-development work may include:

prediction intervals;
uncertainty quantification;
improved severity calibration;
additional climate and event-intensity signals;
additional geospatial features;
continued temporal validation;
model drift monitoring;
automated retraining;
model comparison across disaster regimes; and
improved early-warning performance.
### 38.7 Production Monitoring

A future production platform should monitor:

API availability;
prediction latency;
model errors;
input-data drift;
feature drift;
prediction distribution changes;
model performance over time; and
ingestion failures.
================================================================================
## 40. Recommended Current Operating Model
================================================================================

For the current project phase, the recommended architecture is:

              FEMA / NOAA
                   |
                   v
            Batch Ingestion
                PRIMARY
                   |
                   v
       Common Feature Engineering
                   |
            +------+------+
            |             |
         Model B        Model A
         EARLY         STANDARD
            |             |
            +------+------+
                   |
                   v
                FastAPI
                   |
                   v
              Streamlit
                   |
                   v
            Decision Support

Kafka remains available separately:

Kafka
  |
  v
Experimental ingestion path

It should not be treated as the default operating path until its storage/update architecture has been redesigned.

================================================================================
## 41. Current Platform Status
================================================================================

The current platform provides:

FEMA disaster-data integration;
FEMA Public Assistance integration;
NOAA climate/event-data integration;
primary batch ingestion;
incremental data processing;
common feature engineering;
Model A — Standard Forecast;
Model B — Early Forecast;
ingestion-independent model selection;
FastAPI model serving;
Streamlit scenario analysis;
budget-gap analysis;
Dockerized serving;
validated API health checks;
validated Model A inference;
validated Model B inference; and
a clear future roadmap toward streaming, orchestration, CI/CD, and cloud deployment.

The current platform does not require:

Kafka;
Airflow; or
cloud infrastructure

for its normal forecasting and serving workflow.

The current recommended operating model is therefore:

Batch ingestion + common feature engineering + Model A/Model B + FastAPI + Streamlit + Docker Desktop

with Kafka retained as an experimental alternative.

================================================================================
## 42. License
================================================================================

See:

LICENSE
for licensing information.
