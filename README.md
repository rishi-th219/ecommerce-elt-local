# Local ELT E-Commerce Data Warehouse (Microsoft SQL Server + dbt + Airflow + Metabase)

[![dbt](https://img.shields.io/badge/dbt-Core%201.8%2B-FF694B?logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![MSSQL](https://img.shields.io/badge/Microsoft%20SQL%20Server-2025-CC292B?logo=microsoftsqlserver&logoColor=white)](https://www.microsoft.com/sql-server)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9.1-017CEE?logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![PowerBI](https://img.shields.io/badge/Power%20BI-Ready-F2C811?logo=powerbi&logoColor=black)](https://powerbi.microsoft.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Tests](https://img.shields.io/badge/dbt%20tests-213%20passed-brightgreen)](#dbt-transformation--testing-layer)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A production-grade, 100% free and open-source **ELT Data Warehouse** designed for enterprise analytics engineering portfolios. Built on **Microsoft SQL Server 2025** (with cross-database support for PostgreSQL 16), demonstrating an end-to-end modern data stack: raw bronze landing, JSON-based ELT ingestion, dbt transformations with **Kimball dimensional modeling** and **Slowly Changing Dimensions (SCD Type 2)**, 213 automated tests, Airflow DAG orchestration, and Power BI / Metabase reporting.

---

## Architecture Overview

```
[Python Synthetic Event Generator] (Faker, referential consistency)
                 │ (Raw JSON files)
                 ▼
[Bronze Landing Zone: Local / MinIO S3] (./data/bronze/)
                 │ (pymssql / pyodbc bulk batch ingestion)
                 ▼
[Microsoft SQL Server 2025: "raw" Schema] (NVARCHAR(MAX) JSON landing + DATETIME2 audit)
                 │
                 ▼
[dbt Core Transformations (dbt-sqlserver): Silver to Gold]
       ├── staging:      parse JSON with JSON_VALUE once into tables, enforce ANSI types
       ├── intermediate: sessionize user journeys, calculate line margins & COGS
       └── marts:        Kimball Star Schema
                         dims:    dim_customers (SCD2), dim_products, dim_date
                         facts:   fct_orders (transaction), fct_sessions (transaction),
                                  fct_daily_sales (periodic snapshot)
                         derived: mart_customer_360 (RFM segmentation)
                 ▲
                 │ (Scheduled daily run + health monitoring + alerting)
[Apache Airflow Orchestration] (Webserver + Scheduler running in Docker)
                 │
                 ▼
[Power BI & Metabase Reporting Layer] (Direct connection to marts schema on Port 1433 / 3000)
```

---

## Kimball Star Schema & ER Diagram

![Kimball Star Schema](docs/er_diagram.png)

### Schema Breakdown

| Entity | Schema Layer | Type | Grain | Primary / Natural Keys | Key Measures / Attributes |
|---|---|---|---|---|---|
| **`dim_customers`** | `marts` | SCD Type 2 | 1 row per customer version | `customer_key` (SK), `customer_id` (NK) | `state`, `country`, `valid_from`, `valid_to`, `is_current` (BIT) |
| **`dim_products`** | `marts` | Dimension | 1 row per catalog product | `product_key` (SK), `product_id` (NK) | `sku`, `category`, `unit_cost`, `retail_price`, `baseline_margin_pct` |
| **`dim_date`** | `marts` | Role-Playing | 1 row per calendar day | `date_key` (YYYYMMDD INT) | `full_date`, `day_of_week`, `fiscal_quarter`, `is_weekend` |
| **`fct_orders`** | `marts` | Transactional Fact | 1 row per order line item | `fct_order_item_key` (SK), `order_item_id` | `quantity`, `gross_amount`, `discount_amount`, `net_revenue`, `cogs`, `gross_profit` |
| **`fct_sessions`** | `marts` | Transactional Fact | 1 row per web session | `session_key` (SK), `session_id` | `traffic_source`, `device_type`, `duration_minutes`, `page_views_count`, `order_count`, `session_attributed_revenue` |
| **`fct_daily_sales`** | `marts` | Periodic Snapshot Fact | 1 row per trading day | `date_key` | `order_count`, `units_sold`, `net_revenue`, `gross_profit`, `profit_margin_pct` |
| **`mart_customer_360`** | `marts` | Accumulating Summary | 1 row per customer | `customer_id` (NK), `customer_key` (FK) | `lifetime_net_revenue`, `avg_order_value`, `days_since_last_order`, `rfm_segment` |
| **`int_sessions`** | `intermediate` | Analytical View | 1 row per web session | `session_id` | Feeds `fct_sessions`; sessionization and revenue attribution logic |

> Every dimension carries a Kimball **`-1` Unknown member** (`19000101` for `dim_date`). Facts route unresolved foreign keys to it, so a failed lookup surfaces as an Unknown row instead of a `NULL` that silently drops from an inner-joined BI query.

---

## Architectural & Engineering Rationale

### 1. Why ELT over ETL?
- In traditional ETL, transformations occur in an intermediate compute engine before loading. In this modern **ELT** architecture, raw e-commerce payloads land directly into SQL Server `raw` schema as `NVARCHAR(MAX)` JSON documents with zero upfront loss of fidelity.
- All downstream transformations (casting, schema enforcement, SCD2 window calculations) are managed via **dbt Core**, utilizing SQL Server's native relational engine, tracking transformations in version-controlled SQL, and enabling idempotent replays.

### 2. Why Kimball Star Schema over 3NF or Snowflake?
- **Analytical Performance & Predictability:** Star schemas minimize table joins. Queries against `fct_orders` only require single-hop joins to `dim_customers`, `dim_products`, and `dim_date`.
- **BI Self-Service Usability:** Business users and BI tools (Power BI, Metabase, Tableau) easily navigate flat dimension attributes without deciphering complex normalized bridges.
- **Aggregations:** Additive facts (`net_revenue`, `quantity`, `gross_profit`) roll up cleanly across any dimension attribute (e.g., category, customer state, fiscal quarter).

### 3. Slowly Changing Dimension (SCD Type 2) Rationale
- When customers relocate between states (e.g. moving from New York to Texas), an SCD Type 1 model would overwrite the customer's state, improperly attributing prior historical revenue to the new jurisdiction.
- This warehouse implements **SCD Type 2** on `dim_customers` using surrogate keys (`customer_key`), timestamp validity boundaries (`valid_from`, `valid_to`), and an active status flag (`is_current`).
- `fct_orders` joins to `dim_customers` using point-in-time timestamp matching:
  ```sql
  LEFT JOIN dim_customers dc
      ON m.customer_id = dc.customer_id
      AND m.order_date >= dc.valid_from
      AND (m.order_date < dc.valid_to OR dc.valid_to IS NULL)
  ```
  This guarantees historical reports accurately preserve customer geography at the exact time of order placement.
- **Why hand-rolled SCD2 rather than `dbt snapshot`?** dbt's native snapshot is the right production default — it maintains `dbt_valid_from` / `dbt_valid_to` incrementally, so history survives even when the source system only exposes current state. It is deliberately not used here for two reasons: the source feed already carries the full event history (so history can be *rebuilt* deterministically rather than accumulated, which keeps the project reproducible from a clean database), and the `LEAD()` window logic is the part an interviewer actually wants to see and interrogate. The trade-off is real: a full rebuild is O(all history) where a snapshot is O(new rows), and a snapshot would be the correct choice the moment the source stops replaying past events.
- Three edge cases the dimension handles explicitly:
  - **Replayed source events** are deduplicated on `(customer_id, updated_at)` before hashing, so two identical events cannot collide into one surrogate key.
  - **Early-arriving facts** — the first version of each customer is anchored to `1900-01-01`, so an order that predates the first captured profile row still resolves to a version. This also makes the single point-in-time join above sufficient; no "current version" fallback join is needed.
  - **Unresolvable keys** land on the `-1` Unknown member rather than becoming `NULL`.

### 4. Cross-Engine Portability: Microsoft SQL Server + PostgreSQL
- Built using reusable Jinja macros in `dbt/macros/`:
  - `{{ json_extract('col', 'key') }}`: Generates `JSON_VALUE(col, '$.key')` on SQL Server and `col->>'key'` on PostgreSQL.
  - `{{ date_to_key('col') }}`: Generates `CONVERT(INT, CONVERT(VARCHAR(8), col, 112))` on SQL Server and `TO_CHAR(col, 'YYYYMMDD')::integer` on PostgreSQL.
  - `{{ bool_flag('condition') }}` / `{{ type_bool() }}`: SQL Server has no `BOOLEAN` and PostgreSQL refuses an `INTEGER -> BOOLEAN` cast, so the flag logic lives in one macro instead of being copy-pasted into every model.
  - Each macro raises a compiler error on an unsupported adapter rather than silently falling through to the PostgreSQL branch.
- Both database engines run 100% of staging, intermediate, and marts models and pass the full **213-test automated test suite**.

### 5. Environment Safety
- `generate_schema_name` writes clean layer schemas (`staging`, `intermediate`, `marts`) only on the deployment targets listed in the `prod_targets` project variable. Any other target — a personal `dev` output, or CI — is prefixed (`dev_marts`, `ci_marts`), so two people running `dbt run` against one shared database cannot overwrite each other's tables or production's.
- No credential is committed. `MSSQL_PASSWORD` has no fallback anywhere in the repository: the dbt profile and the loaders both fail loudly if it is unset. In production the same variable names would be injected from a secrets manager (AWS Secrets Manager, Azure Key Vault, Vault) with no file changes.

---

## Repository Structure

```
ecommerce-elt-local/
├── docker-compose.yml           # Multi-service stack: MSSQL 2025, Postgres, Airflow, MinIO, Metabase
├── Dockerfile.airflow           # Airflow 2.9 + ODBC Driver 18 + dbt (postgres & sqlserver adapters)
├── .env.example                 # Environment configuration template (copy to .env)
├── requirements.txt             # Python dependencies for host-level execution
├── LICENSE                      # MIT
├── ingestion/
│   ├── __init__.py
│   ├── config.yaml              # Scale parameters, date ranges, non-secret connection defaults
│   ├── logging_config.py        # Shared stdlib logging setup for every ingestion module
│   ├── generate_data.py         # Faker-based synthetic e-commerce data generator (SCD2 aware)
│   ├── load_to_bronze.py        # Lands raw JSON to ./data/bronze/ and the MinIO bucket
│   ├── load_to_mssql.py         # Transactional batch load of raw JSON into the MSSQL `raw` schema
│   └── load_to_postgres.py      # Transactional batch load of raw JSON into the Postgres `raw` schema
├── dbt/
│   ├── dbt_project.yml          # Project configuration, schema routing, and project variables
│   ├── packages.yml             # dbt_utils package dependency
│   ├── package-lock.yml         # Pinned package resolution
│   ├── profiles.yml.example     # Connection profiles template (mssql / postgres / dev)
│   ├── macros/
│   │   ├── cross_db.sql         # type_bool, bool_flag, bool_literal, days_between, adapter guard
│   │   ├── json_extract.sql     # Cross-engine JSON extraction (JSON_VALUE vs ->>)
│   │   ├── date_to_key.sql      # Cross-engine date-to-integer key macro
│   │   └── generate_schema_name.sql # Target-aware schema routing (prefixes non-prod targets)
│   ├── models/
│   │   ├── sources.yml          # Raw landing source declarations + freshness thresholds
│   │   ├── staging/             # Silver layer: JSON parsing & ANSI type enforcement (tables)
│   │   │   ├── stg_customers.sql
│   │   │   ├── stg_products.sql
│   │   │   ├── stg_sessions.sql
│   │   │   ├── stg_orders.sql
│   │   │   ├── stg_order_items.sql
│   │   │   └── staging.yml
│   │   ├── intermediate/        # Silver-Gold bridge: business calculations (views)
│   │   │   ├── int_sessions.sql
│   │   │   ├── int_order_margins.sql
│   │   │   └── intermediate.yml
│   │   └── marts/               # Gold layer: Kimball dimensional star schema (tables)
│   │       ├── dim_customers.sql      # SCD Type 2 dimension
│   │       ├── dim_products.sql
│   │       ├── dim_date.sql           # Calendar spine (GENERATE_SERIES, var-driven bounds)
│   │       ├── fct_orders.sql         # Transaction fact, order line grain
│   │       ├── fct_sessions.sql       # Transaction fact, session grain
│   │       ├── fct_daily_sales.sql    # Periodic snapshot fact, day grain
│   │       ├── mart_customer_360.sql  # Customer lifetime value + RFM segmentation
│   │       └── marts.yml              # Column docs, schema & foreign key tests
│   └── tests/
│       ├── generic/
│       │   └── expression_is_true.sql   # Portable replacement for the dbt_utils version
│       ├── assert_net_revenue_positive.sql
│       ├── assert_scd2_date_validity.sql
│       ├── assert_scd2_no_overlapping_versions.sql
│       ├── assert_scd2_single_current_version.sql
│       └── assert_fct_orders_ties_to_source.sql
├── airflow/
│   └── dags/
│       └── elt_pipeline_dag.py  # Orchestrates the full end-to-end ELT schedule & alerts
├── docs/
│   ├── er_diagram.png           # Visual Kimball ER diagram
│   ├── generate_er_diagram.py   # Script to regenerate the ER diagram
│   ├── data_dictionary.md       # Comprehensive table and field dictionary
│   └── project_walkthrough_and_linkedin_series.md # Architecture guide & 5-part LinkedIn series
└── README.md
```

---

## Quick Start Guide

### Prerequisites
- [Docker](https://docs.docker.com/engine/install/) (v24+) and [Docker Compose](https://docs.docker.com/compose/) (v2.20+)
- Python 3.10+ — only for running the pipeline from the host instead of from Airflow
- [Microsoft ODBC Driver 18 for SQL Server](https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server) — only for host-level `dbt --target mssql` runs. The Airflow image installs it itself; without it on the host, dbt fails with `libodbc.so.2: cannot open shared object file`.

### 1. Clone and configure

```bash
git clone https://github.com/<your-username>/ecommerce-elt-local.git
cd ecommerce-elt-local
cp .env.example .env          # MSSQL_PASSWORD has no default anywhere - it must be set here
```

### 2. Launch the stack

```bash
docker compose up -d
```

This starts:
- **Microsoft SQL Server 2025**: Port `1433` (`sa` / the password in your `.env`). A one-shot `mssql-init` service creates the `warehouse` database once SQL Server reports healthy.
- **PostgreSQL 16**: Port `5432` (`warehouse` and `airflow` databases, schemas created by `docker/postgres/init.sql`)
- **Airflow Webserver**: Port `8080` (UI: `admin` / `admin`) — `airflow-init` also runs `dbt deps` once, so packages are ready before the first DAG run
- **Airflow Scheduler**: Background task coordinator
- **MinIO S3**: Port `9000` (API), Port `9001` (Console: `minioadmin` / `minioadmin`)
- **Metabase**: Port `3000` (Local BI dashboard)

Every host port is overridable in `.env` (`MSSQL_HOST_PORT`, `POSTGRES_HOST_PORT`, `AIRFLOW_HOST_PORT`, …) if something is already listening locally.

The `ecommerce_elt_pipeline` DAG is unpaused on load and runs `@daily`. To trigger it immediately:

```bash
docker compose exec airflow-scheduler airflow dags trigger ecommerce_elt_pipeline
```

Set `DBT_TARGET=postgres` in `.env` and recreate the Airflow containers to run the same DAG against PostgreSQL instead — one variable switches both the raw loader and the dbt target.

### 3. Or run the pipeline from the host

```bash
# 1. Virtual environment and dependencies
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 2. Load the environment (the loaders and dbt read these variables)
set -a && source .env && set +a

# 3. dbt connection profile and packages (a fresh clone fails without dbt deps)
cp dbt/profiles.yml.example dbt/profiles.yml
dbt deps --project-dir dbt

# 4. Generate synthetic e-commerce data and land it in the bronze zone
python -m ingestion.load_to_bronze

# 5. Load raw JSON into Microsoft SQL Server (creates the `warehouse` database if absent)
python -m ingestion.load_to_mssql

# 6. Build every model and run all 213 tests
dbt build --project-dir dbt --profiles-dir dbt --target mssql

# 7. Optional: verify the raw layer is fresh
dbt source freshness --project-dir dbt --profiles-dir dbt --target mssql
```

To run against PostgreSQL instead, use `python -m ingestion.load_to_postgres` and `--target postgres`.

### 4. Browse the data catalog

```bash
dbt docs generate --project-dir dbt --profiles-dir dbt --target mssql
dbt docs serve --project-dir dbt --profiles-dir dbt --port 8081
```

Port 8081 keeps it clear of the Airflow webserver on 8080. Opens an interactive catalog with every model description, column-level documentation, test coverage, and a clickable DAG lineage graph from `raw.raw_*` through to the marts.

---

## 📘 Documentation

| Document | What it covers |
|---|---|
| [Data Dictionary](docs/data_dictionary.md) | Every table and column, both engines' physical types, grain statements, additivity, and the full test-coverage matrix |
| [Project Walkthrough & LinkedIn Series](docs/project_walkthrough_and_linkedin_series.md) | Layer-by-layer architecture reasoning, interview talk track, and a 5-part storytelling series |
| [ER Diagram](docs/er_diagram.png) | Visual star schema — regenerate with `python docs/generate_er_diagram.py` |
| [dbt catalog](#4-browse-the-data-catalog) | Generated docs site with lineage, descriptions, and test results |

---

## dbt Transformation & Testing Layer

The project runs a **213-test automated quality assurance suite** on every `dbt build`, plus a source freshness check:

- **Uniqueness & Not-Null:** Every surrogate and natural key across the staging, intermediate and marts layers.
- **Referential Integrity:** From every fact to every dimension it references (`fct_orders`, `fct_sessions`, `fct_daily_sales`, `mart_customer_360`), and from staging children to their parents.
- **Domain Validity:** `accepted_values` on `order_status`, `payment_method`, `device_type`, `traffic_source`, product `category`, `day_of_week`, and the RFM scores and segment labels.
- **Range & Arithmetic Validity:** a project-level `expression_is_true` generic test — quantities and prices `> 0`, discounts and revenues `>= 0`, a warning-severity `retail_price >= unit_cost`, and model-level contracts such as `gross_amount - discount_amount = net_revenue`. It replaces `dbt_utils.expression_is_true`, which emits an unaliased `select 1` that SQL Server rejects when dbt materializes the test as a view — exactly the kind of portability bug a dual-engine project exists to surface.
- **Source Freshness:** `dbt source freshness` warns at 24h and errors at 48h on the raw layer's `_ingested_at` watermark, catching a silently stopped ingestion before stale marts reach a dashboard.
- **Custom Business Rules:**
  - `assert_net_revenue_positive` — line item revenue can never be negative after discounts.
  - `assert_scd2_date_validity` — `valid_to > valid_from` for every historical version.
  - `assert_scd2_no_overlapping_versions` — no customer is valid in two versions at once, the condition that would make a point-in-time join double count revenue.
  - `assert_scd2_single_current_version` — exactly one current version per customer.
  - `assert_fct_orders_ties_to_source` — fact revenue reconciles to the source order headers. This is the check that catches a join fan-out; uniqueness tests pass happily on a duplicated grain.

`dbt build` is used rather than `dbt run` followed by `dbt test`: it parses the manifest once and runs each model's tests immediately after that model, so a broken model stops its own descendants instead of poisoning the entire marts layer first.

---

## Sample Analytical Queries for Power BI & SQL Exploration

Connect Power BI or Metabase to Microsoft SQL Server (`server: localhost,1433`, `database: warehouse`, `user: sa`, password: the `MSSQL_PASSWORD` value from your `.env`).

### 1. Revenue & Gross Profit Margin by Product Category
```sql
SELECT
    p.category,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.quantity) AS units_sold,
    SUM(f.gross_amount) AS gross_sales,
    SUM(f.discount_amount) AS discounts,
    SUM(f.net_revenue) AS total_net_revenue,
    SUM(f.gross_profit) AS total_gross_profit,
    CAST(ROUND((SUM(f.gross_profit) / NULLIF(SUM(f.net_revenue), 0) * 100), 2) AS NUMERIC(10,2)) AS profit_margin_pct
FROM marts.fct_orders f
JOIN marts.dim_products p ON f.product_key = p.product_key
WHERE f.order_status = 'completed'
GROUP BY p.category
ORDER BY total_net_revenue DESC;
```

### 2. SCD Type 2 Historical Verification: Sales by Customer State at Order Time
```sql
SELECT TOP 10
    c.state AS customer_state_at_purchase,
    COUNT(DISTINCT f.order_id) AS order_count,
    SUM(f.net_revenue) AS total_revenue
FROM marts.fct_orders f
JOIN marts.dim_customers c ON f.customer_key = c.customer_key
GROUP BY c.state
ORDER BY total_revenue DESC;
```

### 3. Digital Marketing Conversion Attribution
```sql
SELECT
    s.traffic_source,
    s.device_type,
    COUNT(s.session_id) AS total_sessions,
    COUNT(s.converted_order_id) AS converted_sessions,
    CAST(ROUND((CAST(COUNT(s.converted_order_id) AS FLOAT) / NULLIF(COUNT(s.session_id), 0) * 100), 2) AS NUMERIC(10,2)) AS conversion_rate_pct,
    SUM(s.session_attributed_revenue) AS total_revenue
FROM marts.fct_sessions s
GROUP BY s.traffic_source, s.device_type
ORDER BY total_revenue DESC;
```

### 4. Monthly Trading Performance (Periodic Snapshot)
```sql
SELECT
    d.year,
    d.month,
    d.month_name,
    SUM(f.order_count) AS orders,
    SUM(f.units_sold) AS units,
    SUM(f.net_revenue) AS net_revenue,
    SUM(f.gross_profit) AS gross_profit,
    CAST(ROUND(SUM(f.gross_profit) / NULLIF(SUM(f.net_revenue), 0) * 100, 2) AS NUMERIC(10,2)) AS margin_pct
FROM marts.fct_daily_sales f
JOIN marts.dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;
```

### 5. RFM Segment Value Distribution
```sql
SELECT
    rfm_segment,
    COUNT(*) AS customers,
    CAST(ROUND(AVG(lifetime_net_revenue), 2) AS NUMERIC(14,2)) AS avg_lifetime_revenue,
    CAST(ROUND(AVG(avg_order_value), 2) AS NUMERIC(14,2)) AS avg_order_value,
    AVG(days_since_last_order) AS avg_days_since_last_order
FROM marts.mart_customer_360
GROUP BY rfm_segment
ORDER BY avg_lifetime_revenue DESC;
```

---

## License & Author

Built for professional Data & Analytics Engineering portfolios. Released under the [MIT License](LICENSE).

The credentials in `.env.example` are throwaway local development values for a Docker stack that binds to localhost. Nothing secret is committed: `MSSQL_PASSWORD` has no fallback in `profiles.yml`, `config.yaml` or any loader, so the project fails loudly rather than quietly using a default. A production deployment would inject the same variable names from a secrets manager.
