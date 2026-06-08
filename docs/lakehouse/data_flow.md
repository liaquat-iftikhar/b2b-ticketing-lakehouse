# Data Flow: PostgreSQL → Medallion Lakehouse

This document describes how data moves from the operational PostgreSQL database and reseller CSV files through the Bronze → Silver → Gold Delta Lake layers and into the Streamlit reporting dashboard.

---

## 1. End-to-End Overview

```mermaid
flowchart TD
    subgraph Sources["Data Sources"]
        PG[(PostgreSQL\nevent_platform)]
        CSV[CSV Files\ndata/inbound/\nDailySales_YYYYMMDD_*.csv]
    end

    subgraph Orchestration["Airflow Orchestration  ·  DAG per entity  ·  @daily"]
        AF[Apache Airflow 3\nLocalExecutor]
    end

    subgraph Bronze["Bronze Layer  —  Raw Ingestion  (Delta Lake)"]
        B1[organizer_etl/bronze]
        B2[reseller_etl/bronze]
        B3[customer_etl/bronze]
        B4[event_etl/bronze]
        B5[ticket_etl/bronze]
        B6[commission_agreement_etl/bronze]
        B7[sale_etl/bronze]
        B8[reseller_sale_etl/bronze]
    end

    subgraph Silver["Silver Layer  —  Cleaned & Typed  (Delta Lake)"]
        S1[organizer_etl/silver]
        S2[reseller_etl/silver]
        S3[customer_etl/silver]
        S4[event_etl/silver]
        S5[ticket_etl/silver]
        S6[commission_agreement_etl/silver]
        S7[sale_etl/silver]
        S8[reseller_sale_etl/silver]
        SQ[reseller_sale_etl/quarantine/silver]
    end

    subgraph Gold["Gold Layer  —  Business Aggregates  (Delta Lake)"]
        G1[sales_report_etl/gold\nunified sales fact table]
    end

    subgraph Reporting["Reporting Layer"]
        DK[DuckDB\ndelta_scan]
        ST[Streamlit Dashboard\nlocalhost:8501]
    end

    PG -->|JDBC parallel read| AF
    CSV -->|Structured Streaming\navailableNow trigger| AF

    AF -->|SparkSubmitOperator| B1 & B2 & B3 & B4 & B5 & B6 & B7
    AF -->|SparkSubmitOperator| B8

    B1 --> S1
    B2 --> S2
    B3 --> S3
    B4 --> S4
    B5 --> S5
    B6 --> S6
    B7 --> S7
    B8 --> S8
    B8 -.->|invalid rows| SQ

    S1 & S2 & S3 & S4 & S5 & S6 & S7 & S8 --> G1

    G1 --> DK --> ST
```

---

## 2. Source Systems

### 2a. Operational PostgreSQL (`event_platform`)

Seven tables are extracted daily:

| Table | Primary Key | Description |
|---|---|---|
| `organizer` | `organizer_id` | Event organizers |
| `reseller` | `reseller_id` | Ticket reseller companies |
| `customer` | `customer_id` | End customers |
| `event` | `event_id` | Events created by organizers |
| `ticket` | `ticket_id` | Ticket types per event |
| `commission_agreement` | `agreement_id` | Organizer–reseller commission contracts |
| `sale` | `sale_id` | Platform-side ticket sales |

### 2b. Reseller CSV Files (`data/inbound/`)

Vendor daily sales reports dropped as CSV files following the naming convention:

```
DailySales_YYYYMMDD_<reseller_id>.csv
```

The `reseller_id` and `sale_date` are parsed directly from the filename via regex.

---

## 3. Bronze Layer — Raw Ingestion

**Purpose:** Land source data as-is into Delta Lake with lineage metadata. No business transformations.

### 3a. Database tables → Bronze (JDBC batch extract)

**Job:** `src/jobs/core/bronze/database_table_extractor.py` (`JDBCDatabaseExtractor`)   
**Write mode:** `append` with `mergeSchema=true`

Each table is read in full via JDBC (optionally parallelised with partition column + bounds) and three lineage columns are appended before writing:

| Column added | Value |
|---|---|
| `lh_bronze_execution_date` | Airflow logical date (`{{ ds }}`) |
| `lh_bronze_inserted_timestamp` | `current_timestamp()` at write time |
| `lh_bronze_source_database_table` | Source table name string |

```mermaid
sequenceDiagram
    participant AF as Airflow Scheduler
    participant SP as PySpark (SparkSubmitOperator)
    participant PG as PostgreSQL
    participant DL as Delta Lake / Bronze

    AF->>SP: spark-submit database_table_extractor.py\n--table sale --execution_date 2026-06-08
    SP->>PG: JDBC SELECT * FROM sale\n(optional partitioned read)
    PG-->>SP: Raw rows
    SP->>SP: Add lh_bronze_* columns
    SP->>DL: df.write.format("delta").mode("append")\n→ sale_etl/bronze/
```

### 3b. CSV files → Bronze (Spark Structured Streaming)

**Job:** `src/jobs/reseller_sale_etl/bronze/incremental_reseller_data_extractor.py` (`IncrementalResellerDataExtractor`)  
**Write mode:** Streaming append, partitioned by `lh_bronze_execution_date`, `trigger=availableNow`

The job enforces a strict schema contract (`DATA_CONTRACT_SCHEMA`) on ingest so malformed files fail fast. Column names are sanitised (spaces → underscores) and additional fields extracted:

| Column added | Source |
|---|---|
| `reseller_id` | Regex on `input_file_name()` |
| `sale_date` | Regex on `input_file_name()` |
| `lh_bronze_execution_date` | Airflow execution date |
| `lh_bronze_inserted_timestamp` | `current_timestamp()` |

Streaming checkpoints are stored at `reseller_sale_etl/_checkpoints/` to enable exactly-once semantics across re-runs.

---

## 4. Silver Layer — Cleansed & Typed

**Purpose:** Enforce types, deduplicate, rename to business-friendly column names, and upsert idempotently. The Silver layer is the **system of record** for all downstream consumers.

### 4a. Pattern: DB-sourced entities (organizer, reseller, customer, event, ticket, commission_agreement, sale)

All seven share the same structure via `BaseDataPipeline`:

```mermaid
flowchart LR
    B[Bronze Delta\nfiltered by\nexecution_date] --> E[Extract\nspark.read.delta]
    E --> T[Transform\ncast types\ndeduplicate via\nrow_number window]
    T --> L{Silver table\nexists?}
    L -- No --> OW[Write overwrite\nbootstrap]
    L -- Yes --> MG[Delta MERGE\nwhenMatchedUpdateAll\nwhenNotMatchedInsertAll]
```

**Deduplication:** a `Window.partitionBy(pk).orderBy(lh_bronze_inserted_timestamp DESC)` picks the latest row per primary key within the execution partition.

**Upsert key per entity:**

| Entity | Merge key              |
|---|------------------------|
| organizer | `organizer_id`         |
| reseller | `reseller_id`          |
| customer | `customer_id`          |
| event | `event_id`             |
| ticket | `ticket_id`            |
| commission_agreement | `agreement_id`         |
| sale | `sale_id, reseller_id` |

Two Silver lineage columns are added:

| Column | Value |
|---|---|
| `lh_silver_execution_date` | Airflow logical date |
| `lh_silver_inserted_timestamp` | `current_timestamp()` at write time |

### 4b. Reseller CSV → Silver (with quarantine)

**Job:** `src/jobs/reseller_sale_etl/silver/reseller_data_bronze_to_silver_transformer.py`

This pipeline adds a **quarantine branch** absent from the DB-sourced pipelines:

```mermaid
flowchart TD
    B[Bronze Delta] --> E[Extract]
    E --> C[Cast & conform columns\ntransaction_id → String\ntickets_count → Integer\nnet_revenue → Double\nsale_date parsed MMddyyyy]
    C --> CF{Cast failures?}
    CF -- Yes --> Q1[quarantine_reason:\nType casting mismatch]
    CF -- No --> BV{Business rule\nviolation?}
    BV -- "tickets_count ≤ 0\nnet_revenue < 0\ntransaction_id null" --> Q2[quarantine_reason:\nBusiness validation infraction]
    BV -- Pass --> DD[dropDuplicates\ntransaction_id + reseller_id]
    Q1 & Q2 --> QT[Quarantine Delta\nreseller_sale_etl/quarantine/silver/]
    DD --> MG[MERGE into\nreseller_sale_etl/silver/\npartitioned by created_date]
```

---

## 5. Gold Layer — Business Aggregates

**Purpose:** Produce a single denormalised **unified sales fact table** combining platform sales and CSV reseller sales for BI consumption.

**Job:** `src/jobs/sales_report_etl/gold/sales_report_etl.py` (`SalesReportETL`)  
**DAG:** `sales_report_dag` (runs after all Silver DAGs complete)

### 5a. Join graph

```mermaid
flowchart TD
    SS[sale_etl/silver\nplatform sales] --> JOIN1
    RS[reseller_sale_etl/silver\nCSV sales] --> ENRICH

    EV[event_etl/silver] --> ENRICH
    EV --> JOIN1
    TK[ticket_etl/silver] --> JOIN1
    CU[customer_etl/silver] --> JOIN1
    RE[reseller_etl/silver] --> JOIN1
    CA[commission_agreement_etl/silver] --> JOIN1
    CA --> ENRICH

    ENRICH[Enrich reseller sales:\nevent name match on trim+lower\ncommission lookup by organizer+reseller] --> UNION

    JOIN1[Enrich platform sales:\nticket→event→customer→\nreseller→commission star join] --> UNION

    UNION[UNION BY NAME\ndropDuplicates on sale_id + reseller_id] --> GOLD[sales_report_etl/gold/\nunified_sales_fact Delta table]
```

### 5b. Output schema

| Column | Source                                           |
|---|--------------------------------------------------|
| `sale_id` | `sale.sale_id` / `reseller_sale.transaction_id`  |
| `event_name` | event Silver                                     |
| `event_type` | event Silver                                     |
| `event_region` | event Silver                                     |
| `ticket_category` | ticket Silver (null for CSV path)                |
| `tickets_count` | `sale.quantity` / `reseller_sale.tickets_count`  |
| `total_amount` | `sale.total_amount` / `reseller_sale.net_revenue` |
| `commission_amount` | pre-computed                                     |
| `commission_rate_applied` | commission_agreement Silver                      |
| `sales_channel` | `sale.channel` / `reseller_sale.sales_channel`   |
| `customer_first_name` | customer Silver / CSV field                      |
| `customer_last_name` | customer Silver / CSV field                      |
| `reseller_id` | from source                                      |
| `sale_date` | from source                                      |
| `reseller_location` | reseller Silver country / CSV office_location    |
| `source` | `"platform"` or `"csv_import"`                   |

---

## 6. Reporting Layer

**No Spark required.** The Streamlit dashboard reads Gold Delta files directly using DuckDB's `delta_scan()` function — a lightweight columnar query engine that understands the Delta transaction log.

```mermaid
flowchart LR
    G[sales_report_etl/gold/\nDelta Lake files\non local disk or S3] --> DK[DuckDB\ndelta_scan]
    DK --> ST[Streamlit\nsales_report_dashboard.py\nlocalhost:8501]
    ST --> U[Analyst]
```

Dashboard tabs powered by SQL queries over the Gold table:

| Tab | Query pattern |
|---|---|
| Weekly Channel Trends | `SUM(total_amount) GROUP BY week, sales_channel` |
| YoY February Comparison | `SUM(total_amount) GROUP BY year WHERE month = 2` |
| Commission Correlation | Commission rate vs. revenue scatter |
| Regional Popularity | Sales by `event_region` |

---

## 7. Lakehouse Directory Structure

```
data/lakehouse/                         ← DYNACONF_STORAGE__BASE_PATH
│
├── organizer_etl/
│   ├── bronze/                         ← Delta table (append)
│   ├── silver/                         ← Delta table (MERGE upsert)
│   └── _checkpoints/
│
├── reseller_etl/          bronze/ silver/
├── customer_etl/          bronze/ silver/
├── event_etl/             bronze/ silver/
├── ticket_etl/            bronze/ silver/
├── commission_agreement_etl/ bronze/ silver/
├── sale_etl/              bronze/ silver/
│
├── reseller_sale_etl/
│   ├── bronze/                         ← Delta table (streaming append)
│   ├── silver/                         ← Delta table (MERGE, partitioned by created_date)
│   ├── quarantine/
│   │   └── silver/                     ← Rejected rows with quarantine_reason
│   └── _checkpoints/
│       └── reseller_sale_etl_to_bronze/
│
└── sales_report_etl/
    └── gold/                           ← Unified sales fact Delta table (MERGE)
```

On AWS, `data/lakehouse/` maps to `s3a://YOUR-BUCKET/lakehouse/` — all paths are resolved by `LakehousePathResolver` and support both local and S3 URIs transparently.

---

## 8. Airflow DAG Dependency Map

Each entity DAG runs independently on a `@daily` schedule. DAGs publish an **Airflow Dataset** on success; downstream DAGs may consume those events.

```mermaid
flowchart TD
    subgraph Seed["Seed DAGs  (schedule: None — run once)"]
        OL[operational_db_data_loader]
        RL[reseller_vendor_data_loader]
    end

    subgraph Extract["Extractor DAGs  (@daily)"]
        D1[organizer_data_extractor_dag\nbronze → silver]
        D2[reseller_data_extractor_dag\nbronze → silver]
        D3[customer_data_extractor_dag\nbronze → silver]
        D4[event_data_extractor_dag\nbronze → silver]
        D5[ticket_data_extractor_dag\nbronze → silver]
        D6[commission_agreement_data_extractor_dag\nbronze → silver]
        D7[sale_data_extractor_dag\nbronze → silver]
        D8[reseller_sale_data_extractor_dag\nbronze → silver + quarantine]
    end

    subgraph Report["Report DAG  (@daily)"]
        G[sales_report_dag\nsilver → gold]
    end

    OL -->|seeds PostgreSQL| D1 & D2 & D3 & D4 & D5 & D6 & D7
    RL -->|seeds data/inbound/| D8

    D1 & D2 & D3 & D4 & D5 & D6 & D7 & D8 -->|Airflow Dataset events| G
```

### Within each extractor DAG

Every extractor DAG follows the same two-task pattern:

```
start_gate → bronze_ingest_<entity> → silver_transform_<entity> → end_gate
```

Both Spark steps use `SparkSubmitOperator` with `conn_id=spark_local` and receive `--execution_date {{ ds }}` so all runs are idempotent and partition-safe.

---

## 9. Config & Path Resolution

All paths and connection IDs are resolved at runtime via Dynaconf:

| Config key | Default (local) | Override (production) |
|---|---|---|
| `storage.base_path` | `data/lakehouse` | `s3a://bucket/lakehouse` |
| `storage.inbound_path` | `data/inbound` | `s3a://bucket/inbound` |
| `airflow.spark_conn_id` | `spark_local` | EMR cluster URI |
| `airflow.postgres_conn_id` | `postgres_event_platform` | RDS endpoint |

`LakehousePathResolver.resolve(layer, etl_name)` builds all three path variants (target, checkpoint, quarantine) from a single base path, making every job portable between local disk and S3 with zero code changes.
