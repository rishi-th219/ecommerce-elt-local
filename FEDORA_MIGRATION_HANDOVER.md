# 🚀 Agent Rebuild & Migration Handover Runbook
## Local ELT E-Commerce Data Warehouse (MSSQL 2025 • dbt Core • Airflow • Metabase)

> **Audience**: AI Coding Agents (Gemini Flash, Claude, Antigravity) & Rishi Thakur (`rishiithakur219@gmail.com`).  
> **Purpose**: Complete, fail-safe instructions to rebuild, provision, and verify the entire data warehouse stack from scratch on any new machine (Fedora, Ubuntu/Debian, macOS, or WSL2) after all containers, volumes, and images have been removed.

---

## 1. Quick Architecture & Ground Truth Summary

* **Primary Warehouse**: **Microsoft SQL Server 2025 Developer Edition** running in Docker/Podman (`mssql-server`, port `1433`).
* **Portability Engine**: **PostgreSQL 16** (`ecommerce-postgres`, port `5432`) & Airflow metadata DB.
* **Orchestrator**: **Apache Airflow 2.9.1** (`ecommerce-airflow-webserver` on port `8080`, `ecommerce-airflow-scheduler`).
* **Transformation**: **dbt-core 1.12.3** with `dbt-sqlserver 1.11.1` and `dbt-postgres 1.11.0` (14 models, **213 automated tests**).
* **Object Storage**: **MinIO** S3 API on port `9000`, Web Console on port `9001`.
* **BI & Analytics**: **Metabase** on port `3000` (reading from `marts` schema).
* **Ground Truth Files**:
  * [PROJECT_STUDY_GUIDE.md](file:///home/rishi21/Desktop/Jobhunt/Projects/ecommerce-elt-local/PROJECT_STUDY_GUIDE.md): Master technical runbook and Kimball interview preparation.
  * [LINKEDIN_POSTS.md](file:///home/rishi21/Desktop/Jobhunt/Projects/ecommerce-elt-local/LINKEDIN_POSTS.md): 5-part storytelling series for LinkedIn personal brand.
  * [IMPROVEMENT_SUGGESTIONS.md](file:///home/rishi21/Desktop/Jobhunt/Projects/ecommerce-elt-local/IMPROVEMENT_SUGGESTIONS.md): Audit log of 44 implemented architectural optimizations.
  * [dbt/models/marts/marts.yml](file:///home/rishi21/Desktop/Jobhunt/Projects/ecommerce-elt-local/dbt/models/marts/marts.yml): Schema contract and 213 test definitions.

---

## 2. System Prerequisites & Dependencies

### Operating System Packages
Ensure either **Docker** or **Podman** is installed on the target machine:

#### On Fedora / RHEL / CentOS:
```bash
# 1. Install Podman and compose tooling
sudo dnf install -y podman podman-compose podman-docker

# 2. Enable and start user socket (required for docker-compose emulation)
systemctl --user enable --now podman.socket

# 3. (Optional) If running ingestion or dbt directly from host Python:
sudo dnf install -y unixODBC-devel python3-devel gcc gcc-c++
# Add Microsoft repository and install msodbcsql18:
curl -fsSL https://packages.microsoft.com/config/fedora/40/prod.repo | sudo tee /etc/yum.repos.d/mssql-release.repo
sudo ACCEPT_EULA=Y dnf install -y msodbcsql18
```

#### On Ubuntu / Debian:
```bash
# 1. Install Docker & Compose plugin
sudo apt-get update && sudo apt-get install -y docker.io docker-compose-v2
sudo usermod -aG docker $USER && newgrp docker

# 2. (Optional host tools):
sudo apt-get install -y unixodbc-dev python3-dev build-essential
```

---

## 3. Environment Setup & Configuration

From the repository root (`ecommerce-elt-local`):

### 3.1 Provision `.env`
If `.env` does not exist, copy from `.env.example`:
```bash
cp .env.example .env
```
Ensure the following variables are present in `.env`:
```ini
MSSQL_SERVER=localhost
MSSQL_PORT=1433
MSSQL_DB=warehouse
MSSQL_USER=sa
MSSQL_PASSWORD=P@ssword#@219#
MSSQL_HOST_PORT=1433

POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=warehouse
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_HOST_PORT=5432
AIRFLOW_DB=airflow

AIRFLOW_IMAGE_NAME=apache/airflow:2.9.1
AIRFLOW_UID=50000
AIRFLOW_USER=admin
AIRFLOW_PASSWORD=admin
AIRFLOW_EMAIL=admin@example.com
AIRFLOW_HOST_PORT=8080
DBT_TARGET=mssql

MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
MINIO_ENDPOINT=http://minio:9000
MINIO_BUCKET=bronze
MINIO_API_HOST_PORT=9000
MINIO_CONSOLE_HOST_PORT=9001

METABASE_HOST_PORT=3000
DBT_PROFILES_DIR=/opt/airflow/dbt
INGESTION_LOG_LEVEL=INFO
```

### 3.2 Python Host Virtual Environment (Optional, for host CLI runs)
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
*(Note: If you do not install ODBC on the host, you can run all pipeline and dbt commands inside the `ecommerce-airflow-webserver` container as shown in Section 5).*

---

## 4. ⚠️ CRITICAL TRAP RESOLUTION (Read Before Launching)

### Trap 1: Fedora/RHEL SELinux Volume Mount Permission Denied
* **Symptom**: Airflow or dbt fails on startup with `PermissionError: [Errno 13] Permission denied: '/opt/airflow/dbt/dbt_project.yml'` or `ls: cannot access ... Permission denied`.
* **Root Cause**: Host files are labeled `user_home_t` instead of `container_file_t`, blocking container read/write access under SELinux.
* **Instant Fix**: Run this command on the host repository before starting containers:
  ```bash
  chcon -R -t container_file_t dbt/ airflow/ ingestion/ data/
  ```

### Trap 2: Host Log Directory Ownership (Rootless Podman / Docker)
* **Symptom**: Airflow scheduler crashes because it cannot write DAG task logs to `./airflow/logs`.
* **Root Cause**: In rootless container setups, container user `50000` lacks write permission to the host directory.
* **Instant Fix**:
  ```bash
  chmod -R 777 airflow/logs data/bronze
  ```

### Trap 3: Microsoft SQL Server Password Complexity
* **Symptom**: `mssql-server` container exits immediately with code 1.
* **Root Cause**: SQL Server enforces strict password complexity (min 8 chars, uppercase, lowercase, digits, symbols).
* **Rule**: Keep `MSSQL_PASSWORD=P@ssword#@219#`. Do not change to simple passwords like `password` or `admin`.

### Trap 4: Port Conflicts
* **Symptom**: `bind: address already in use` for ports 1433, 5432, 8080, or 3000.
* **Instant Fix**: If local Postgres or Airflow is running on the host, change `POSTGRES_HOST_PORT=5433` or `MSSQL_HOST_PORT=1434` in `.env`.

---

## 5. Step-by-Step Rebuild Sequence

Follow these 5 steps sequentially to rebuild and verify the stack:

### Step 1: Start the Docker / Podman Stack
```bash
# Set SELinux context on host directories
chcon -R -t container_file_t dbt/ airflow/ ingestion/ data/

# Ensure host write permissions for logs and bronze landing
chmod -R 777 airflow/logs data/bronze

# Build and start all 6 services in background
docker compose up -d
# (Or with podman: podman-compose up -d)
```

### Step 2: Verify Service Health
Wait ~30 seconds for SQL Server to pass its health check, then verify:
```bash
docker ps
```
You must see all containers active:
* `mssql-server` (Up, healthy, port `1433`)
* `ecommerce-postgres` (Up, healthy, port `5432`)
* `ecommerce-minio` (Up, ports `9000`, `9001`)
* `ecommerce-metabase` (Up, port `3000`)
* `ecommerce-airflow-webserver` (Up, port `8080`)
* `ecommerce-airflow-scheduler` (Up)

### Step 3: Seed Bronze Data and Load into MSSQL Raw Schema
Execute the ingestion scripts inside the Airflow webserver container (avoids needing host ODBC drivers):
```bash
# 1. Generate synthetic events and land in ./data/bronze/
docker exec ecommerce-airflow-webserver bash -c "python -m ingestion.load_to_bronze"

# 2. Atomic batch load bronze JSON into SQL Server raw schema (raw.raw_*)
docker exec ecommerce-airflow-webserver bash -c "python -m ingestion.load_to_mssql"
```

### Step 4: Execute Transformations & Full 213-Test Suite
Run unified `dbt build` inside the container:
```bash
# 1. Verify dbt connectivity to SQL Server 2025
docker exec ecommerce-airflow-webserver bash -c "cd /opt/airflow/dbt && dbt debug --target mssql --profiles-dir /opt/airflow/dbt"

# 2. Check source freshness SLA
docker exec ecommerce-airflow-webserver bash -c "cd /opt/airflow/dbt && dbt source freshness --target mssql --profiles-dir /opt/airflow/dbt"

# 3. Build models & execute all 213 tests
docker exec ecommerce-airflow-webserver bash -c "cd /opt/airflow/dbt && dbt build --target mssql --profiles-dir /opt/airflow/dbt"

# 4. Standalone test verification (must pass 213 tests)
docker exec ecommerce-airflow-webserver bash -c "cd /opt/airflow/dbt && dbt test --target mssql --profiles-dir /opt/airflow/dbt"
```
*Expected terminal output*:
```text
Finished running 213 data tests in 0 hours 0 minutes and 9.68 seconds (9.68s).
Completed successfully
Done. PASS=213 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=213
```

### Step 5: (Optional) Verify PostgreSQL Cross-Engine Portability
To prove 100% portability on Postgres 16:
```bash
# 1. Load raw data into PostgreSQL raw schema
docker exec ecommerce-airflow-webserver bash -c "python -m ingestion.load_to_postgres"

# 2. Build and test on PostgreSQL
docker exec ecommerce-airflow-webserver bash -c "cd /opt/airflow/dbt && dbt build --target postgres --profiles-dir /opt/airflow/dbt"
```

---

## 6. Accessing UIs & Verification Dashboards

| Service | URL | Default Credentials | Description |
|---|---|---|---|
| **Airflow Web UI** | `http://localhost:8080` | `admin` / `admin` | Production DAG: `ecommerce_elt_pipeline`. Trigger manual runs or inspect task SLAs. |
| **Metabase BI** | `http://localhost:3000` | Setup on first visit or existing DB | Connected to MSSQL `warehouse.marts` for category margins, state maps, and customer RFM cohorts. |
| **MinIO S3 Console**| `http://localhost:9001` | `minioadmin` / `minioadmin` | S3 bucket `bronze` object browser (`landing/latest/*.json`). |
| **SQL Server 2025** | `localhost:1433` | `sa` / `P@ssword#@219#` | Query directly via Azure Data Studio, DBeaver, or VS Code SQL extension. |
| **PostgreSQL 16** | `localhost:5432` | `postgres` / `postgres` | Portability target and Airflow metadata database (`warehouse`, `airflow`). |

---

## 7. Immediate Agent Startup Checklist (When Handing Off)

When an incoming agent (Gemini Flash or Claude) starts a session:
1. **Never ask Rishi what this project is.** Everything is documented here.
2. Run `docker ps` to verify container health.
3. If containers were removed or down, follow Section 5 to execute `chcon`, `docker compose up -d`, and verify `dbt test`.
4. Report status in 2-3 lines and prompt Rishi on the active next milestone:
   * **LinkedIn Storytelling Series**: Review/publish Post 2 (Raw JSON ELT vs Cleaning on Ingest) in `LINKEDIN_POSTS.md`.
   * **Interview Prep**: Run mock interview drills from `PROJECT_STUDY_GUIDE.md` (Kimball modeling, SCD2 window logic, fan-out fixes).
   * **Reporting Layer**: Metabase questions/dashboards and documentation.
