# B2B Ticketing Platform — ETL Lakehouse

A medallion-architecture data platform for a B2B event ticketing business. Operational data is extracted from PostgreSQL, processed through Bronze → Silver → Gold Delta Lake layers via PySpark, orchestrated by Apache Airflow, and surfaced through a Streamlit BI dashboard.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│  Operational DB (PostgreSQL)          Reseller Vendors CSV Files        │
│  organizer · reseller · customer      (data/inbound/)                   │
│  event · ticket · sale · agreement                                      │
└────────────────┬────────────────────────────┬───────────────────────────┘
                 │                            │
                 ▼                            ▼
        ┌────────────────────────────────────────┐
        │   Bronze Layer  (raw Delta tables)     │
        │   JDBC extract + CSV ingest via Spark  │
        └────────────────────┬───────────────────┘
                             │
                             ▼
        ┌────────────────────────────────────────┐
        │   Silver Layer  (cleaned + typed)      │
        │   Deduplication, schema enforcement    │
        └────────────────────┬───────────────────┘
                             │
                             ▼
        ┌────────────────────────────────────────┐
        │   Gold Layer    (business aggregates)  │
        │   Sales report fact table              │
        └────────────────────┬───────────────────┘
                             │
                             ▼
                   ┌─────────────────┐
                   │  Streamlit BI   │
                   │  Dashboard      │
                   │  (DuckDB reads  │
                   │   Delta files)  │
                   └─────────────────┘
```

**Orchestration:** Apache Airflow 3 (LocalExecutor)  
**Processing:** PySpark 4 + Delta Lake 4  
**Config:** Dynaconf — environment-switched TOML + env-var overrides  

---

## Project Layout

```
.
├── config/
│   ├── settings.toml          # Dynaconf config (default / local / production)
│   └── .secrets.toml          # DB passwords — gitignored
├── dags/
│   ├── common/
│   │   ├── dag_config.py      # Single import for all DAG constants
│   │   └── datasets.py        # Airflow Dataset triggers
│   └── <entity>_dag/          # One DAG per entity (organizer, customer, …)
├── src/
│   ├── config/
│   │   ├── app_config.py      # Typed dataclasses (AppConfig, SparkConfig, …)
│   │   └── config_loader.py   # ConfigLoader — builds AppConfig from Dynaconf
│   ├── jobs/
│   │   ├── core/              # BaseDataPipeline ABC + SparkSessionFactory
│   │   └── <entity>_etl/      # Bronze / Silver / Gold PySpark jobs per entity
│   └── utils/generator/       # Fake-data generators for local dev seeding
├── reporting/
│   └── sales_report_dashboard.py   # Streamlit dashboard
├── Dockerfile.airflow         # Airflow + Java 17 + PySpark image
├── Dockerfile.reporting       # Lightweight Streamlit image
├── docker-compose.yml         # Full local stack
├── requirements-airflow.txt   # Airflow container deps
└── requirements-reporting.txt # Reporting container deps
```

---

## Local Setup

### Prerequisites

| Tool | Version |
|------|---------|
| Docker Desktop | ≥ 4.x |
| Docker Compose | ≥ 2.x (bundled with Desktop) |
| Python | 3.12+ (for running generators/scripts outside Docker) |

### 1 — Clone and configure secrets

```bash
git clone <repo-url>
cd Iftikhar-liaquat

# Create config/.secrets.toml fill in passwords
```

`config/.secrets.toml` minimum content:

```toml
[default.database]
password = "postgres"

[local.database]
password = "postgres"
```

### 2 — Create the local `.env` file

```bash
cp .env.example .env   # if an example exists, otherwise create:
```

`.env` contents (local dev defaults — do **not** commit):

```dotenv
ENV_FOR_DYNACONF=local

DYNACONF_DATABASE__PORT=5432
DYNACONF_DATABASE__NAME=event_platform
DYNACONF_DATABASE__USER=postgres
DYNACONF_DATABASE__PASSWORD=postgres
```

> **Note:** `DYNACONF_DATABASE__HOST` is intentionally omitted from `.env`.  
> Locally it defaults to `localhost` (from `settings.toml [local]`).  
> Inside Docker it is overridden to `operational-postgres` via `docker-compose.yml`.

### 3 — Build and start the full stack

```bash
docker compose build --no-cache   # first time or after Dockerfile changes
docker compose up -d
```

Services started:

| Service | URL / Port | Purpose |
|---------|-----------|---------|
| `airflow-webserver` | http://localhost:8080 | Airflow UI (admin / admin) |
| `operational-postgres` | localhost:5432 | Source event platform DB |
| `airflow-postgres` | localhost:5433 | Airflow metadata DB |
| `reporting` | http://localhost:8501 | Streamlit BI dashboard |

### 4 — Seed the operational database

**Recommended:** trigger the `operational_db_data_loader` DAG from the Airflow UI at http://localhost:8080. This is a one-off DAG (schedule: `None`) that generates realistic organizers, resellers, customers, events, tickets, and sales records and inserts them directly into PostgreSQL.

1. Open the Airflow UI → search for `operational_db_data_loader`
2. Click the ▶ **Trigger DAG** button
3. Wait for all tasks to turn green

> The DAG runs `src/utils/generator/operational_db/orchestrator.py` inside the scheduler container, so no local Python setup is needed.

### 5 — Generate reseller CSV files

**Recommended:** trigger the `reseller_vendor_data_loader` DAG from the Airflow UI. This DAG generates daily CSV sales files from reseller vendors and writes them to `data/inbound/`, which is the source path for `reseller_sale_data_extractor_dag`.

1. Open the Airflow UI → search for `reseller_vendor_data_loader`
2. Click the ▶ **Trigger DAG** button
3. Wait for all tasks to turn green

CSV files land in `data/inbound/` (mounted into the Airflow containers at `/opt/airflow/data/inbound/`).

> Both seeding DAGs only need to be run once before executing the main ETL pipeline.

### 6 — Run the ETL pipeline

In the Airflow UI (http://localhost:8080) enable and trigger the DAGs in this order:

1. `operational_db_data_loader` — seeds PostgreSQL source data *(step 4)*
2. `reseller_vendor_data_loader` — generates reseller CSV files *(step 5)*
3. Entity extractors — run in any order, they are independent:
   - `organizer_data_extractor_dag`
   - `reseller_data_extractor_dag`
   - `customer_data_extractor_dag`
   - `event_data_extractor_dag`
   - `ticket_data_extractor_dag`
   - `commission_agreement_data_extractor_dag`
   - `sale_data_extractor_dag`
   - `reseller_sale_data_extractor_dag`
4. `sales_report_dag` — builds the Gold layer fact table (run after all extractors complete)

### 7 — View the dashboard

Open http://localhost:8501 — the Streamlit dashboard reads Gold layer Delta files directly via DuckDB (no Spark required at query time).

---

## Local Development (without Docker)

For editing and testing individual jobs locally:

```bash
# Create a virtual environment
python -m venv .venv
source .venv/bin/activate

# Install deps
pip install -r requirements-airflow.txt

# Start only the database
docker compose up -d operational-postgres

# Run a specific PySpark job
python -m src.jobs.reseller_sale_etl.bronze.reseller_data_bronze_to_silver_transformer \
  --execution_date 2026-06-01

# Run the Streamlit dashboard locally
pip install streamlit plotly duckdb pandas statsmodels
streamlit run reporting/sales_report_dashboard.py
```

---

## Configuration Reference

All config lives in `config/settings.toml` with three layers:

| Layer | Applied when |
|-------|-------------|
| `[default]` | Always — base values |
| `[local]` | `ENV_FOR_DYNACONF=local` (default for Docker + local dev) |
| `[production]` | `ENV_FOR_DYNACONF=production` |

Any key can be overridden at runtime with an env var:

```
DYNACONF_<SECTION>__<KEY>=value
```

Examples:

```bash
DYNACONF_DATABASE__HOST=my-rds-host.amazonaws.com
DYNACONF_STORAGE__BASE_PATH=s3a://my-bucket/lakehouse
DYNACONF_DATABASE__PASSWORD=supersecret
```

---

## AWS Cloud Deployment

### Infrastructure required

| Component | AWS Service |
|-----------|------------|
| Orchestration | Amazon MWAA (Managed Airflow) or self-hosted on ECS/EC2 |
| Spark processing | Amazon EMR Serverless or EMR on EKS |
| Lakehouse storage | Amazon S3 |
| Source database | Amazon RDS for PostgreSQL |
| Dashboard hosting | ECS Fargate (Streamlit container) or EC2 |
| Secrets | AWS Secrets Manager or SSM Parameter Store |

### 1 — Configure production settings

Update `config/settings.toml` `[production]` section with your real values, or override entirely via env vars (recommended for secrets):

```toml
[production.storage]
type         = "s3"
base_path    = "s3a://YOUR-BUCKET/lakehouse"
inbound_path = "s3a://YOUR-BUCKET/inbound"

[production.database]
host = "your-rds-endpoint.rds.amazonaws.com"
port = 5432
name = "event_platform"
user = "postgres"
```

Set secrets via env vars (never commit to git):

```bash
export ENV_FOR_DYNACONF=production
export DYNACONF_DATABASE__PASSWORD=<from Secrets Manager>
export AWS_ACCESS_KEY_ID=<key>
export AWS_SECRET_ACCESS_KEY=<secret>
export AWS_DEFAULT_REGION=eu-west-1
```

### 2 — Push Docker images to ECR

```bash
# Authenticate
aws ecr get-login-password --region eu-west-1 \
  | docker login --username AWS --password-stdin \
      <account-id>.dkr.ecr.eu-west-1.amazonaws.com

# Build and push Airflow image
docker build -f Dockerfile.airflow \
  -t <account-id>.dkr.ecr.eu-west-1.amazonaws.com/ticketing-airflow:latest .
docker push <account-id>.dkr.ecr.eu-west-1.amazonaws.com/ticketing-airflow:latest

# Build and push reporting image
docker build -f Dockerfile.reporting \
  -t <account-id>.dkr.ecr.eu-west-1.amazonaws.com/ticketing-reporting:latest .
docker push <account-id>.dkr.ecr.eu-west-1.amazonaws.com/ticketing-reporting:latest
```

### 3 — Option A: Amazon MWAA (managed Airflow)

1. Upload the `dags/` folder to the MWAA S3 bucket:
   ```bash
   aws s3 sync dags/ s3://YOUR-MWAA-BUCKET/dags/
   aws s3 sync src/  s3://YOUR-MWAA-BUCKET/dags/src/   # so DAGs can import src.*
   aws s3 sync config/ s3://YOUR-MWAA-BUCKET/dags/config/
   ```
2. Upload `requirements-airflow.txt` to the MWAA bucket and reference it in the MWAA environment config.
3. Set MWAA environment variables:
   ```
   ENV_FOR_DYNACONF           = production
   DYNACONF_DATABASE__HOST    = your-rds-endpoint
   DYNACONF_DATABASE__PASSWORD = <from Secrets Manager>
   DYNACONF_STORAGE__BASE_PATH = s3a://YOUR-BUCKET/lakehouse
   AIRFLOW_CONN_SPARK_LOCAL   = {"conn_type":"spark","host":"local[*]"}
   AIRFLOW_CONN_POSTGRES_EVENT_PLATFORM = {"conn_type":"postgres","host":"..."}
   ```
4. Configure Spark connections to point to your EMR cluster endpoint instead of `local[*]`.

### 4 — Option B: Self-hosted on ECS / EC2

Deploy `docker-compose.yml` to an EC2 instance, replacing local Postgres endpoints with RDS and adjusting the storage paths to S3:

```bash
# On your EC2 instance
git clone <repo-url>
cd Iftikhar-liaquat

# Set production env vars
export ENV_FOR_DYNACONF=production
export DYNACONF_DATABASE__HOST=your-rds.amazonaws.com
export DYNACONF_DATABASE__PASSWORD=$(aws secretsmanager get-secret-value \
  --secret-id db-password --query SecretString --output text)
export DYNACONF_STORAGE__BASE_PATH=s3a://YOUR-BUCKET/lakehouse

# Attach an IAM role to the EC2 instance with S3 + RDS access,
# then start the stack (no AWS keys needed if using instance role)
docker compose up -d
```

### 5 — S3 IAM policy (minimum required)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::YOUR-BUCKET",
        "arn:aws:s3:::YOUR-BUCKET/*"
      ]
    }
  ]
}
```

### 6 — Switching from local[*] to EMR Spark

Update the Airflow Spark connection (`spark_local`) to point to your EMR master node:

```json
{
  "conn_type": "spark",
  "host": "spark://emr-master.your-cluster.amazonaws.com",
  "port": 7077
}
```

Or use `yarn` mode if submitting through the EMR cluster manager:

```json
{
  "conn_type": "spark",
  "host": "yarn",
  "extra": {"deploy-mode": "cluster", "master": "yarn"}
}
```

---

## Useful Commands

```bash
# View all running services
docker compose ps

# Follow Airflow scheduler logs
docker compose logs -f airflow-scheduler

# Follow Spark job logs inside the scheduler
docker exec -it airflow_scheduler bash

# Rebuild a single service after code changes (no full restart needed for DAG edits)
docker compose build airflow-scheduler && docker compose up -d airflow-scheduler

# Wipe all data and start fresh
docker compose down -v
docker compose up -d

# Check Dynaconf resolved config
docker exec -it airflow_scheduler \
  python -c "from src.config.config_loader import ConfigLoader; import pprint; pprint.pprint(ConfigLoader.load().__dict__)"
```

---
