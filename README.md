```
# SaaS Product Analytics Pipeline

End-to-end data pipeline: **S3 → Snowpipe → Snowflake → dbt → Airflow → Streamlit**, with CI/CD via GitHub Actions.

## Architecture
```

┌─────────────┐ ┌──────────────┐ ┌─────────────────────────────────────────────┐  
│ Synthetic │ │ S3 │ │ Snowflake │  
│ Events │────▶│ /bronze/ │────▶│ Snowpipe (AUTO_INGEST via SNS) │  
│ (JSON) │ │ (raw JSON) │ │ ┌─────────────────────────────────────┐ │  
└─────────────┘ └──────────────┘ │ │ Bronze: bronze_events (VARIANT) │ │  
│ └──────────────┬──────────────────────┘ │  
│ │ dbt │  
│ ┌──────────────▼──────────────────────┐ │  
│ │ Silver: stg_events, stg_users │ │  
│ │ (views, typed, filtered) │ │  
│ └──────────────┬──────────────────────┘ │  
│ │ dbt │  
│ ┌──────────────▼──────────────────────┐ │  
│ │ Gold: fact_sessions, dim_users, │ │  
│ │ dim_features (star schema) │ │  
│ └──────────────┬──────────────────────┘ │  
└───────────────────┼──────────────────────────┘  
│  
┌───────────────────▼──────────────────────────┐  
│ Streamlit Dashboard │  
│ KPIs · Event Breakdown · Feature Adoption │  
└──────────────────────────────────────────────┘

Orchestration: Airflow DAG (Docker Compose)  
CI/CD: GitHub Actions (SQLFluff → dbt compile → dbt test)

## Tech Stack

```
| Layer | Technology |
|-------|------------|
| Source | Synthetic JSON events (boto3 / manual upload) |
| Storage (raw) | AWS S3 |
| Ingestion | Snowpipe (event-driven via SNS) |
| Warehouse | Snowflake (Bronze → Silver → Gold) |
| Transformation | dbt (staging views + marts tables) |
| Orchestration | Apache Airflow (Docker Compose) |
| Data Quality | dbt tests (19 checks) + row count anomaly gate |
| Dashboard | Streamlit + snowflake-connector-python |
| CI/CD | GitHub Actions + SQLFluff |
```




## Project Structure

saas-analytics-pipeline/  
├── .github/  
│ └── workflows/  
│ └── pipeline.yml # CI/CD: lint → compile → test  
├── airflow/  
│ ├── dags/  
│ │ └── saas_pipeline.py # DAG: refresh → dbt run → dbt test → row check  
│ └── Dockerfile # Airflow + dbt-snowflake + snowflake-connector  
├── dbt/  
│ ├── models/  
│ │ ├── staging/  
│ │ │ ├── sources.yml # Points to ANALYTICS.BRONZE.bronze_events  
│ │ │ ├── stg_events.sql # Silver: typed, filtered events (view)  
│ │ │ ├── stg_users.sql # Silver: typed, filtered users (view)  
│ │ │ └── schema.yml # Tests: unique, not_null, accepted_values  
│ │ └── marts/  
│ │ ├── fact_sessions.sql # Gold: fact table (table)  
│ │ ├── dim_users.sql # Gold: user dimension (table)  
│ │ ├── dim_features.sql # Gold: feature dimension (table)  
│ │ └── schema.yml # Tests: relationships, not_null  
│ ├── refresh_pipe.py # ALTER PIPE REFRESH via snowflake-connector  
│ ├── row_count_check.py # Anomaly detection gate  
│ ├── profiles.yml # dbt profile (env-var based for CI)  
│ ├── dbt_project.yml # Project config  
│ ├── seeds/  
│ ├── macros/  
│ └── tests/  
├── streamlit/  
│ └── app.py # Dashboard: KPIs, charts, filters  
├── data/  
│ └── generate_events.py # Synthetic data generator (10K events + 500 users)  
├── docker-compose.yml # Airflow + Postgres  
├── .env # Local credentials (gitignored)  
└── .gitignore

```
## Prerequisites

- Python 3.11+
- Docker + Docker Compose
- AWS account (S3, SNS, IAM)
- Snowflake account (trial or paid)
- GitHub account

## Setup

### 1. Clone & Configure

```bash
git clone https://github.com/Naivebayes-clas/saas-analytics-pipeline.git
cd saas-analytics-pipeline
```

Create `.env` (gitignored):

```
# .env
SNOWFLAKE_ACCOUNT=db89749.eu-west-2.aws
SNOWFLAKE_USER=YOUR_USER
SNOWFLAKE_PASSWORD=YOUR_PASSWORD
SNOWFLAKE_ROLE=TRANSFORMER
SNOWFLAKE_WAREHOUSE=TRANSFORMING
SNOWFLAKE_DATABASE=ANALYTICS
SNOWFLAKE_SCHEMA=BRONZE

AWS_REGION=us-west-2
S3_BUCKET=your-saas-events-bucket

POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
AIRFLOW_SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
FERNET_KEY=$(python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
AIRFLOW_SECRET_KEY=$(python -c "import secrets; print(secrets.token_hex(32))")
```

### 2. AWS (S3 + SNS + IAM)

```
# S3 bucket
aws s3 mb s3://your-saas-events-bucket --region us-west-2

# SNS topic (for Snowpipe auto-ingest)
aws sns create-topic --name SnowpipeIngestTopic --region us-west-2

# IAM role for Snowflake to assume
# (Create via console: IAM → Roles → External AWS account,
#  attach S3 read-only policy on your bucket)

# S3 event notification → SNS
# (Console: S3 → bucket → Properties → Event notifications →
#  Object Created → prefix "bronze/" → SNS topic)
# (SNS topic must have a resource-based policy allowing s3.amazonaws.com to publish)
```

### 3. Snowflake

Run in Snowflake SQL Editor (as `ACCOUNTADMIN`):

```
-- Database, schema, storage integration, role grants
-- Stage, table, pipe (see dbt/models/staging/sources.yml for table reference)
```

Key objects:

- **Storage Integration** (`s3_int`) — links Snowflake to the IAM role

- **External Stage** (`bronze_stage`) — points to `s3://your-bucket/bronze/`

- **Bronze Table** (`bronze_events`) — `VARIANT` column for raw JSON

- **Snowpipe** (`bronze_pipe`) — `AUTO_INGEST = TRUE`, watches SNS topic

### 4. Generate & Upload Data

```
pip install pandas numpy
python data/generate_events.py
aws s3 cp data/ s3://your-saas-events-bucket/bronze/ --recursive --include "*.json"
```

Snowpipe auto-ingests within ~30 seconds. Verify:

```
SELECT COUNT(*) FROM ANALYTICS.BRONZE.bronze_events;
-- Expected: ~10,500 rows
```

### 5. dbt (Silver + Gold)

```
pip install dbt-snowflake
cd dbt
dbt run --profiles-dir .
dbt test --profiles-dir .
# Expected: 5 models built, 19 tests PASS
```

### 6. Airflow (Docker Compose)

```
docker compose up -d --build
# Wait ~30s for init
# Open http://localhost:8080 (admin / admin)
# Trigger DAG: saas_analytics_pipeline → ▶
```

DAG tasks: `refresh_snowpipe → dbt_build → dbt_test → row_count_check`

### 7. Streamlit Dashboard

```
pip install streamlit snowflake-connector-python
streamlit run streamlit/app.py
# Opens http://localhost:8501
```

### 8. CI/CD (GitHub Actions)

Push to `main` → workflow auto-runs:

1. **SQLFluff lint** — style + syntax check

2. **dbt compile** — structural validation

3. **dbt test** — 19 data quality tests

Add GitHub Secrets: `SF_ACCOUNT`, `SF_USER`, `SF_PASSWORD`, `SF_ROLE`, `SF_WAREHOUSE`, `SF_DATABASE`, `SF_SCHEMA`

## Data Quality Tests (19 total)

| Model           | Tests                                                                                            |
| --------------- | ------------------------------------------------------------------------------------------------ |
| `stg_events`    | unique(event_id), not_null(event_id, user_id, event_type, event_ts), accepted_values(event_type) |
| `stg_users`     | unique(user_id), not_null(user_id, email, plan_tier)                                             |
| `fact_sessions` | unique(event_id), not_null(event_id, session_id, user_id), relationships(user_id → dim_users)    |
| `dim_users`     | unique(user_id), not_null(user_id, email, plan_tier)                                             |
| `dim_features`  | unique(feature_id), not_null(feature_id)                                                         |

## Medallion Architecture

| Layer      | Materialization    | Purpose                                 |
| ---------- | ------------------ | --------------------------------------- |
| **Bronze** | Table (Snowpipe)   | Raw, immutable, untyped VARIANT         |
| **Silver** | View (dbt staging) | Typed, filtered, business rules applied |
| **Gold**   | Table (dbt marts)  | Star schema, dashboard-ready            |
