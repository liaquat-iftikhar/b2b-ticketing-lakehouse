# Engineering Roadmap & Platform Enhancements

---

## 1. Code Architecture & CI/CD Lifecycle

### 1.1 Decouple Orchestration (DAGs) from Compute (Spark Applications)
* **Current State:** Monorepo architecture bundling DAG definitions, utility scripts, and Spark data transformation files.
* **Target State:** Split the repository into two isolated codebases:
  1. `event-platform-orchestration`: Contains purely lightweight Airflow DAG files, configuration blocks, and connection settings.
  2. `event-platform-compute`: Contains PySpark transformation engines, custom modules, data validation code, and internal dependency configurations (`requirements.txt`, setup wheels).
* **Impact:** Prevents dependency drift, isolates build/deploy blast radiuses, and aligns with corporate security models where data workflows and infrastructure execute on separate cadences.

### 1.2 Enterprise CI/CD Pipeline Automation
* **Target State:** Build a unified automation pipeline utilizing **GitHub Actions** or **GitLab CI** triggered on target branch pull requests.
* **Pipeline Stages:**
  * **Linting/Formatting:** Enforce code standards using `ruff` and `black`.
  * **Testing:** Automatically trigger the `unittest` verification frameworks.
  * **Build & Publish:** Build and tag custom Docker images, publishing them directly to a private container registry (AWS ECR or Docker Hub).
  * **Deployment:** Automate asset syncing (DAG definitions to an S3 bucket mapped to Airflow workers; Spark applications to target compute runtimes).

### 1.3 Externalize Secret Management
* **Current State:** Environment configurations and localized keys are isolated inside flat, local `.secrets.toml` storage configurations.
* **Target State:** Move all operational parameters, database credentials, and API connection strings out of local storage disk files entirely. Configure Airflow to leverage an enterprise-grade native **Secret Backend** (such as AWS Secrets Manager or HashiCorp Vault) to inject variables directly into memory contexts at task runtime.

---

## 2. Reliability, Quality Assurance & Data Contracts

### 2.1 Matrix-Level Testing Strategy
* **Orchestration Validation:** Expand the current `unittest` coverage to catch structural code regressions, checking for DAG cycle errors, verifying task id uniqueness, and testing task configuration objects across different mock variables.
* **Compute Logic Isolation:** Implement local Spark context testing frameworks to test the internal transformation rules inside `database_table_extractor.py` against predefined mock static test data frames. This isolates pipeline calculation logic testing from database or network connectivity dependencies.

### 2.2 Data Contracts & Ingestion Scopes
* **Target State:** Implement formal data contract validation rules at the ingestion boundaries.
* **Impact:** Intercepts corrupted, malformed, or structural schema changes on incoming third-party reseller CSV files (`DailySales_MMDDYYYY_RESELLERID.CSV`) *before* they can enter the storage layer. Any files that fail schema validation are routed to a quarantine path, and an alert is sent, keeping the raw Bronze data tier clear of bad rows.

### 2.3 Layer-Bound Data Quality Circuit Breakers
* **Target State:** Deploy data validation rules between each stage of the Medallion architecture layers.
* **Implementation:** After a data task finishes executing, run data quality assertions to verify critical data rules (e.g., primary key uniqueness constraints on `customer_id`, non-null check enforcement on financial rows like `total_amount`, and logical boundary checks on dates).
* **Circuit Breaker Rule:** If a critical data assertion fails, the downstream workflow immediately halts, stopping corrupt records from moving into the Gold data layer or impacting the Streamlit business dashboard.

---

## 3. Infrastructure Scaling & Performance

### 3.1 Migrate to Distributed Cloud-Native Compute
* **Current State:** PySpark is running inside the container cluster mapped to a single host machine local master process (`--master local[*]`).
* **Target State:** Reconfigure the system properties to offload computing workloads to elastic cloud infrastructure. Transition the `SparkSubmitOperator` properties to target ephemeral clusters via **AWS EMR Serverless**, managed **Databricks** environments, or by spinning up dynamic containers on a remote cluster using the **KubernetesPodOperator**.

### 3.2 Delta Lake File System Optimization
* **Target State:** Schedule a dedicated administrative maintenance DAG to manage table degradation and optimize access speeds over time.
* **Task Automation:**
  * **`OPTIMIZE`**: Compact fragmented, small file segments within the parquet structure into clean, large block formats to speed up I/O times.
  * **`Z-ORDER BY`**: Multi-dimensionally cluster data around high-frequency filter columns (such as `sale_date` and `region`) to optimize storage layouts for analytics queries.
  * **`VACUUM`**: Clean out outdated transaction logs and files to control storage space and costs.

---

## 4. Lineage, Observability & Data Governance

### 4.1 Real-Time Slack & Incident Alerting Hooks
* **Target State:** Centralize pipeline execution tracking across the data stack.
* **Implementation:** Integrate global notification macros using Airflow's native `on_failure_callback` hooks. If a task crashes, a formatted alert is sent to an engineering team's Slack or PagerDuty channel, including the exact task ID, processing timestamp, error details, and a direct URL to the logs for faster debugging.

### 4.2 Automated End-to-End Data Lineage Tracking
* **Target State:** Implement automated data lineage mapping across all active data pipelines.
* **Implementation:** Enable **OpenLineage** integration inside the Airflow 3 and Spark engine configurations to feed metadata directly into an open-source lineage server (such as **Marquez**).
* **Impact:** Automatically traces the full life cycle of data elements as they move from source Postgres tables, through individual Spark processing steps, and down to the final physical Delta files, without requiring manually maintained static architecture diagrams.

### 4.3 Scale Governance with Automated Data Catalogs
* **Target State:** Deploy an open-source data catalog and discovery tool (such as **DataHub** or **Amundsen**) on top of the lakehouse ecosystem.
* **Impact:** Automatically indexes Delta Lake schemas, column types, and data lineage history, exposing a searchable, user-friendly data dictionary interface that lets corporate data analysts query, audit, and understand table contents independently.

---

## 5. Release Management & GitFlow Governance

### 5.1 Enforce Strict Branching and Release Hygiene
* **Target State:** Transition repository operations to a strict GitFlow deployment framework to eliminate configuration leakage and protect production resources.
* **Branch Taxonomy Implementation:**
  * `main`: Reflects production-stable environments. Direct branch pushes are blocked via branch protection rules. Code modifications enter exclusively via squashed and reviewed pull requests coming from stabilization branches.
  * `develop`: The continuous integration hub matching the Staging layer. Features are merged here to undergo integration testing passes.
  * `feature/*` and `bugfix/*`: Short-lived task-specific branches branched off and merged back into `develop`.
  * `release/*`: Dedicated release branches created from `develop` when preparing for a production drop. Permitted exclusively for hot fixes, schema adjustments, and release tracking.
* **Automated Tag Triggers:** Production deployments are decoupled from branch updates and are executed strictly when a signed, semantic version tag (e.g., `v1.2.0`) is published to the upstream repository.

---

## 6. Infrastructure as Code (IaC) via Terraform

### 6.1 Declarative Underlying Resource Provisioning
* **Target State:** Replace all manual cloud or container infrastructure setup procedures with completely declarative **Terraform** configurations.
* **Infrastructure Scope Tracking:**
  * **Networking Blueprint:** Provision secure, isolated Virtual Private Clouds (VPCs), custom public/private subnet architectures, and NAT Gateway routing layouts.
  * **Storage Bounds:** Deploy the decoupled Medallion S3 storage buckets (`bronze`, `silver`, `gold`) utilizing server-side bucket encryption (KMS) and explicit public access blocks.
  * **Compute and Orchestration:** Automate the provisioning of execution instances (AWS EMR Serverless compute nodes, managed Airflow environments, and transactional metadata relational databases).
  * **Security Layering:** Build granular, least-privilege IAM access policies, customer-managed KMS key matrices, and restricted network security groups.

### 6.2 Isolated Remote State Architecture
* **Implementation:** Infrastructure state files must be hosted in an encrypted remote storage layer using atomic write-locking (via Amazon DynamoDB) to prevent concurrent state collisions or data drift during pipeline execution runs.

### 6.3 Environmental Isolation Parameterization
* **Implementation:** Leverage parameterized modules alongside targeted `.tfvars` variable profiles (e.g., `staging.tfvars`, `prod.tfvars`) to guarantee that identical infrastructure blueprints can be stamped out across different scaling tiers without manual intervention.

## 7. Data Privacy Governance & GDPR Compliance

### 7.1 PII Pseudonymization and Data Masking Pipelines
* **Current State:** Inbound transactional tables (`CUSTOMER`, `ORGANIZER`, `RESELLER`) ingest raw personally identifiable information (PII) such as full names, email addresses, phone numbers, and physical addresses in plaintext.
* **Target State:** Enforce a strict data obfuscation boundary at the entry point of the Silver layer to satisfy GDPR data minimization mandates.
* **Implementation:** Implement deterministic cryptographic hashing (e.g., SHA-256 salted with a rotated secret key) or Format-Preserving Encryption (FPE) for primary keys and identifiers like `email` and `phone`. Plaintext attributes must either be isolated in a restricted-access lookup table or completely stripped if they provide zero downstream analytical value to the Gold layer or the Streamlit reporting dashboard.

### 7.2 Right to be Forgotten (Data Erasure & Crypto-Shredding)
* **Target State:** Build a compliant, automated process to handle GDPR Article 17 ("Right to Erasure") requests across immutable Medallion storage layers within the 30-day statutory window.
* **Implementation Framework:**
  * **Delta Lake Erasure & Vacuuming:** Standard `DELETE` statements in Delta Lake only append new transaction log states, leaving historical data exposed in parquet files for time travel. The platform must schedule automated purge scripts that execute `DELETE FROM` commands targeting requested IDs, immediately followed by a strict `VACUUM` command with retention checks disabled to permanently erase physical data blocks from cloud storage.
  * **Crypto-Shredding Strategy:** As an alternative for large historical datasets, encrypt each individual customer's PII rows using a unique, customer-specific data encryption key (DEK) managed via a KMS. To fulfill an erasure request, destroy that specific customer's DEK. This immediately renders all historical log entries across Bronze, Silver, and Gold layers completely unreadable without requiring full table rewrites.

### 7.3 Role-Based Access Control (RBAC) and Audit Logging
* **Target State:** Restrict internal platform access to a strict need-to-know basis and maintain a continuous, immutable compliance audit trail.
* **Implementation:** Enforce column-level data masking and row-level filtering within the Gold analytical models and the Streamlit BI application. Business users must interact purely with anonymized, aggregate metrics. Direct access to unmasked Silver layers must be blocked using IAM row policies, and all data read/write events must be logged to an isolated, write-once-read-many (WORM) security tracking bucket for compliance reviews.