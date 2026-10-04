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

## Bugs Fixed

A log of real issues encountered and resolved during build. Each one is a lesson in the tooling.

### AWS / S3 / SNS

| #   | Bug                                                                     | Root Cause                                                    | Fix                                                                        |
| --- | ----------------------------------------------------------------------- | ------------------------------------------------------------- | -------------------------------------------------------------------------- |
| 1   | `PutBucketNotificationConfiguration` → "Unable to validate destination" | SNS topic had no resource-based policy allowing S3 to publish | Added policy with `aws:SourceAccount` (not `aws:SourceOwner`) on the topic |
| 2   | Snowpipe not auto-ingesting files                                       | S3 event notification was never configured                    | Created notification: `s3:ObjectCreated:*` → prefix `bronze/` → SNS topic  |

### Snowflake

| #   | Bug                                                         | Root Cause                                                                                        | Fix                                                                                                          |
| --- | ----------------------------------------------------------- | ------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| 3   | `CREATE TABLE` → "Unsupported data type METADATA$FILE_NAME" | `METADATA$FILE_NAME` is a table function (used in `COPY INTO`), not a column type                 | Used plain `VARCHAR` / `TIMESTAMP_NTZ` columns; moved metadata extraction to the pipe's `COPY INTO` subquery |
| 4   | `USE DATABASE ANALYTICS` silently failed                    | `TRANSFORMER` role had no `USAGE` on the database                                                 | `GRANT USAGE ON DATABASE ANALYTICS TO ROLE TRANSFORMER`                                                      |
| 5   | `USE ROLE TRANSFORMER` → "not assigned to executing user"   | Role was created but never granted to the user                                                    | `GRANT ROLE TRANSFORMER TO USER BARNAP`                                                                      |
| 6   | `GRANT INSERT ON SCHEMA ...` → syntax error                 | `INSERT`/`SELECT` are table-level, not schema-level                                               | Changed to `GRANT INSERT ON ALL TABLES IN SCHEMA ...` + `FUTURE TABLES`                                      |
| 7   | `CREATE STAGE` → "Insufficient privileges"                  | Missing `CREATE STAGE` on schema                                                                  | `GRANT CREATE STAGE ON SCHEMA ANALYTICS.BRONZE TO ROLE TRANSFORMER`                                          |
| 8   | dbt `CREATE VIEW` → "Insufficient privileges"               | Missing `CREATE VIEW` on schema                                                                   | `GRANT CREATE VIEW ON SCHEMA ANALYTICS.BRONZE TO ROLE TRANSFORMER`                                           |
| 9   | `404 Not Found` on login-request                            | Wrong account identifier (tried `IS70418`, `MFEHTLV-IS70418`)                                     | Correct identifier is `db89749.eu-west-2.aws` (from browser URL, not the app URL)                            |
| 10  | SSL cert mismatch                                           | Account field had full hostname → connector appended `.snowflakecomputing.com` again, doubling it | Use short form: `db89749.eu-west-2.aws` (connector adds the domain)                                          |
| 11  | `connections.toml` → "writable by group or others"          | File permission was `664`                                                                         | `chmod 600 ~/.snowflake/connections.toml`                                                                    |

### dbt

| #   | Bug                                                           | Root Cause                                                                                                                                                                                           | Fix                                                                                                          |
| --- | ------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| 12  | "no profile was specified"                                    | Missing `profile:` line in `dbt_project.yml`                                                                                                                                                         | Added `profile: saas_analytics`                                                                              |
| 13  | "No module named dbt.adapters.snowflake"                      | Adapter not installed                                                                                                                                                                                | `pip install dbt-snowflake`                                                                                  |
| 14  | "depends on a node named 'bronze_events' which was not found" | `{{ ref() }}` only works for dbt-created models                                                                                                                                                      | Created `sources.yml` and used `{{ source('bronze', 'bronze_events') }}`                                     |
| 15  | `TypeError: can not serialize 'SnowflakeRelation' object`     | (a) Nested `saas_analytics/` project confused the parser; (b) version mismatch (core 1.11 + adapter 1.12); (c) inline `tests=[...]` in `{{ config() }}` stored a `SnowflakeRelation` in the manifest | Removed nested project; pinned `dbt-core==1.12.5` + `dbt-snowflake==1.12.1`; moved all tests to `schema.yml` |
| 16  | `PermissionError` on `target/partial_parse.msgpack`           | Container user (airflow) and host user (barnap86) had different UIDs on the same mounted dir                                                                                                         | `chmod -R 777 dbt/` + `--no-partial-parse` flag                                                              |
| 17  | `TRY_CAST(VARIANT AS TIMESTAMP_NTZ)` → compilation error      | Snowflake can't `TRY_CAST` directly from VARIANT to TIMESTAMP                                                                                                                                        | Cast through VARCHAR: `TRY_CAST(data:event_ts::VARCHAR AS TIMESTAMP_NTZ)`                                    |

### Data / Timestamps

| #   | Bug                                             | Root Cause                                                                                                          | Fix                                                                       |
| --- | ----------------------------------------------- | ------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------- |
| 18  | `year 58712 is out of range` (Python connector) | `event_ts` / `signup_ts` stored as epoch **milliseconds** (13-digit int); `::TIMESTAMP_NTZ` misinterprets the scale | `TO_TIMESTAMP_NTZ(data:event_ts::NUMBER, 3)` — the `3` means milliseconds |
| 19  | `Unknown function TO_TIMESTAMP_MS`              | That function doesn't exist in Snowflake                                                                            | Use `TO_TIMESTAMP_NTZ(value, 3)` instead                                  |

### Airflow / Docker

| #   | Bug                                                        | Root Cause                                                               | Fix                                                                                         |
| --- | ---------------------------------------------------------- | ------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------- |
| 20  | `dbt: command not found` in container                      | Base `apache/airflow` image doesn't include dbt                          | Custom `Dockerfile`: `FROM apache/airflow:2.10.4` + `pip install dbt-snowflake`             |
| 21  | `pip install` as root → "Please use 'airflow' user"        | Airflow image blocks pip for root                                        | `USER airflow` before `RUN pip install` in Dockerfile                                       |
| 22  | `--profiles-dir: Path '/home/airflow/.dbt' does not exist` | `profiles.yml` was on host, not in container                             | Copied into `dbt/` folder + `--profiles-dir /opt/airflow/dbt` flag                          |
| 23  | `admin is not a valid role`                                | Role name is case-sensitive in Airflow 2.10+                             | Use `Admin` (capital A)                                                                     |
| 24  | Python `SyntaxError: '(' was never closed` in DAG          | Nested escaped quotes (`\"`) inside triple-quoted `bash_command` strings | Extracted inline Python into separate `.py` files (`refresh_pipe.py`, `row_count_check.py`) |

### Streamlit

| #   | Bug                                               | Root Cause                                                | Fix                                                            |
| --- | ------------------------------------------------- | --------------------------------------------------------- | -------------------------------------------------------------- |
| 25  | `KeyError: 'session_id'`                          | Snowflake connector returns column names in **UPPERCASE** | `df.columns = df.columns.str.lower()` after building DataFrame |
| 26  | `module 'streamlit' has no attribute 'pie_chart'` | `st.pie_chart` requires Streamlit ≥ 1.29                  | Replaced with `matplotlib` pie chart                           |

### GitHub / CI-CD

| #   | Bug                                                                               | Root Cause                                                        | Fix                                                                                                                               |
| --- | --------------------------------------------------------------------------------- | ----------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| 27  | Push blocked: "Push cannot contain secrets" (AWS keys)                            | `sns_aws.txt` with Access Key ID + Secret was committed           | `git rm --cached` + `git filter-branch` to purge from history + force push + rotate keys                                          |
| 28  | Push blocked: "refusing to allow PAT to create workflow without `workflow` scope" | Personal Access Token lacked `workflow` scope                     | Regenerated PAT with `repo` + `workflow` scopes                                                                                   |
| 29  | CI: "Could not find profile named 'saas_analytics'"                               | `profiles.yml` was in `.gitignore` (contained hardcoded password) | Rewrote `profiles.yml` to use `{{ env_var('SF_ACCOUNT') }}` etc.; committed the template; injected real values via GitHub Secrets |
