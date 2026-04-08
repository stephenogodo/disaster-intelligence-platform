# FEMA Real-Time Disaster Data Streaming Pipeline

## Overview

This project builds a **real-time data engineering pipeline** that streams large-scale disaster datasets from the **FEMA Open Data API** into **Apache Kafka**, enabling scalable downstream analytics, machine learning, and cloud deployment.

The goal is to demonstrate how modern data platforms evolve:

**Local Development → Containerisation → Cloud Infrastructure → ML Production Systems**

---

## Project Vision

This repository represents a **production-style streaming data platform** designed to simulate how real-world organizations ingest, process, store, and analyze continuously growing datasets.

The pipeline:

1. Extracts disaster data from FEMA APIs
2. Streams data continuously via Kafka
3. Processes events in real time
4. Stores structured datasets for analytics
5. Enables future Machine Learning and forecasting workflows

---

## Architecture Evolution

### Phase 1 — Local Streaming (Completed ✅)

* Python Kafka Producer streams FEMA API data
* Kafka topic acts as streaming buffer
* Kafka Consumer processes data
* Batch persistence to local storage

```
FEMA API → Kafka Producer → Kafka Topic → Kafka Consumer → Local Data Lake
```

---

### Phase 2 — Docker Environment (Completed ✅)

The entire system runs inside containers:

* Kafka
* Zookeeper
* Producer
* Consumer

Benefits:

* Reproducible environment
* Easy onboarding
* Infrastructure parity
* Portable deployment

```
Docker Compose → Kafka Ecosystem → Streaming Pipeline
```

---

### Phase 3 — Cloud Infrastructure (Future Work 🚀)

The pipeline will migrate to a cloud environment such as **AWS**.

#### Planned Cloud Architecture

```
FEMA API
   ↓
Kafka Producer (EC2 / ECS)
   ↓
Managed Kafka (MSK)
   ↓
Stream Processing
   ↓
Data Lake (S3)
   ↓
Analytics + Machine Learning
```

#### Target Cloud Services

* AWS EC2 / ECS — container hosting
* Amazon MSK — managed Kafka cluster
* Amazon S3 — data lake storage
* AWS Glue — schema catalog
* AWS Athena — SQL analytics
* IAM — security & access control
* CloudWatch — monitoring

---

### Phase 4 — Workflow Orchestration with Airflow (Future Work 🚀)

Apache Airflow will manage pipeline automation.

Planned DAGs:

* Start producer jobs
* Monitor Kafka health
* Trigger batch processing
* Run ML training jobs
* Data quality checks
* Automated retries

```
Airflow Scheduler
      ↓
Kafka Jobs + ETL + ML Pipelines
```

Example use cases:

* Scheduled ingestion
* Incremental processing
* Backfills
* Pipeline observability

---

### Phase 5 — Machine Learning Platform (Future Work 🚀)

Streaming data enables predictive intelligence.

Planned ML Applications:

* Disaster frequency prediction
* Funding estimation models
* Incident severity classification
* Time-series forecasting
* Risk scoring dashboards

Proposed ML Stack:

* Python
* Scikit-Learn / XGBoost
* Feature Store
* Model Registry
* Batch + Real-time inference

```
Kafka → Feature Engineering → ML Training → Prediction API
```

---

## Project Folder Structure

---
fema-ml-system/
│
├── README.md                            #DONE, BUT NEEDS CONSTANT UPDATE AS THE PROJECT PROGRESSES
├── docker-compose.yml
├── requirements.txt                     #DONE, BUT NEEDS CONSTANT UPDATE AS THE PROJECT PROGRESSES
├── .gitignore                           # DONE 
│
├── airflow/
│   ├── dags/
│   │   └── fema_pipeline.py              # FUTURE WORK
│   └── Dockerfile
│
├── ingestion/                             # DONE
│   ├── producer.py
│   └── consumer.py
│
├── feature_store/
│   ├── feature_repo/
│   │   ├── feature_store.yaml              #SOON TO BE DONE
│   │   └── features.py
│
├── ml/
│   ├── train.py
│   ├── evaluate.py
│   ├── predict.py
│   └── pipeline.py                          #SOON TO BE DONE
│
├── monitoring/
│   ├── drift.py                             # FUTURE WORK
│   └── performance.py
│
├── api/
│   ├── main.py
│   ├── schemas.py                           # SOON TO BE DONE
│   └── Dockerfile
│
├── dashboard/
│   ├── app.py                               # SOON TO BE DONE
│   └── Dockerfile
│
├── infrastructure/                          # FUTURE CLOUD SETUP
│   ├── terraform/
│   ├── aws/
│   └── monitoring/
│
|── test/
|   |── test_transform
│   ├── test_features 
│
├── .github/                                   #DONE, BUT NEEDS CONSTANT UPDATE AS THE PROJECT PROGRESSES
│   └── workflows/
│       └── ci.yml
|─ data/                                                 
│   ├── raw/
│   ├── processed/
│   └── features.parqueta                        # DONE
│   
|── notebooks/
│   └── exploratory_analysis.ipynb               #IN PROGRESS



## How to Run Locally

### 1. Start Kafka

```
docker-compose up -d
```

### 2. Run Producer

```
python producer/kafka_producer.py
```

### 3. Run Consumer

```
python consumer/kafka_consumer.py
```

---

## Resume-Safe Streaming

The consumer supports:

* Kafka offset tracking
* Restart without data loss
* Continuous ingestion
* Massive dataset handling

You can safely stop the pipeline and resume later.

---

## Data Characteristics

* Massive continuously expanding dataset
* Real-time streaming ingestion
* Event-based architecture
* Schema-versioned messages

---

## Engineering Concepts Demonstrated

* Event-driven architecture
* Streaming pipelines
* Kafka producers & consumers
* Data serialization
* Batch processing from streams
* Containerization with Docker
* Fault-tolerant ingestion
* Resume-safe processing

---

## Future Enhancements

* Cloud deployment (AWS)
* Managed Kafka (MSK)
* Apache Airflow orchestration
* Real-time dashboards
* ML prediction services
* Data warehouse integration
* CI/CD automation
* Observability & monitoring

---

## Why This Project Matters

Modern data platforms are **never built all at once**.

This project demonstrates the real industry journey:

```
Laptop → Containers → Cloud → Automated Pipelines → Machine Learning Platform
```

---

## Author

**Stephen Ogodo**
Data Scientist | Data Engineer | Streaming Systems Enthusiast

---

## License

MIT License
# FEMA_ML_PROJECT