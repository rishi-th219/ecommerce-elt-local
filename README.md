# Local ELT E-Commerce Data Warehouse (PostgreSQL + dbt + Airflow + Metabase)

[![dbt](https://img.shields.io/badge/dbt-Core%201.8%2B-FF694B?logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9.1-017CEE?logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Metabase](https://img.shields.io/badge/Metabase-Reporting-509EE3?logo=metabase&logoColor=white)](https://www.metabase.com/)
[![Tests](https://img.shields.io/badge/dbt%20tests-63%20passed-brightgreen)](#dbt-transformation--testing-layer)

A production-grade, 100% free and open-source **ELT Data Warehouse** designed for enterprise analytics engineering portfolios. Demonstrates an end-to-end modern data stack with raw bronze landing, JSON-based ELT ingestion, dbt transformations with **Kimball dimensional modeling** and **Slowly Changing Dimensions (SCD Type 2)**, automated test suites, Airflow DAG orchestration, and BI reporting in Metabase.

---

## Architecture Overview

```
[Python Synthetic Event Generator] (Faker, referential consistency)
                 │ (Raw JSON files)
                 ▼
[Bronze Landing Zone: Local / MinIO S3] (./data/bronze/)
                 │ (psycopg2 bulk batch ingestion)
                 ▼
[PostgreSQL: "raw" Schema] (JSONB landing tables + ingest audit metadata)
                 │
                 ▼
[dbt Core Transformations: Silver to Gold]
       ├── staging:      parse JSONB, enforce types, rename/clean columns
       ├── intermediate: sessionize user journeys, calculate line margins & COGS
       └── marts:        Kimball Star Schema (dim_customers SCD2, dim_products, dim_date, fct_orders)
                 ▲
                 │ (Scheduled daily run + health monitoring + alerting)
[Apache Airflow Orchestration] (Webserver + Scheduler running in Docker)
                 │
                 ▼
[Metabase / Power BI Reporting Layer] (Direct connection to marts schema)
```

---

## Kimball Star Schema & ER Diagram

![Kimball Star Schema](docs/er_diagram.png)

### Schema Breakdown

| Entity | Schema Layer | Type | Grain | Primary / Natural Keys | Key Measures / Attributes |
|---|---|---|---|---|---|
| **`dim_customers`** | `marts` | SCD Type 2 | 1 row per customer version | `customer_key` (SK), `customer_id` (NK) | `state`, `country`, `valid_from`, `valid_to`, `is_current` |
| **`dim_products`** | `marts` | Dimension | 1 row per catalog product | `product_key` (SK), `product_id` (NK) | `sku`, `category`, `unit_cost`, `retail_price`, `baseline_margin_pct` |
| **`dim_date`** | `marts` | Role-Playing | 1 row per calendar day | `date_key` (YYYYMMDD) | `full_date`, `day_of_week`, `fiscal_quarter`, `is_weekend` |
| **`fct_orders`** | `marts` | Transactional Fact | 1 row per order line item | `fct_order_item_key` (SK), `order_item_id` | `quantity`, `gross_amount`, `discount_amount`, `net_revenue`, `cogs`, `gross_profit` |
| **`int_sessions`** | `intermediate` | Analytical View | 1 row per web session | `session_id` | `traffic_source`, `device_type`, `duration_minutes`, `is_converted` |

---

## Architectural & Engineering Rationale

### 1. Why ELT over ETL?
- In traditional ETL, transformations occur in an intermediate compute engine before loading. In this modern **ELT** architecture, raw e-commerce payloads land directly into PostgreSQL `raw` schema as `JSONB` documents with zero upfront loss of fidelity.
- All downstream transformations (casting, schema enforcement, SCD2 window calculations) are managed via **dbt Core**, utilizing the database's native relational engine, tracking transformations in version-controlled SQL, and enabling idempotent replays.

### 2. Why Kimball Star Schema over 3NF or Snowflake?
- **Analytical Performance & Predictability:** Star schemas minimize table joins. Queries against `fct_orders` only require single-hop joins to `dim_customers`, `dim_products`, and `dim_date`.
- **BI Self-Service Usability:** Business users and BI tools (Power BI, Metabase, Tableau) easily navigate flat dimension attributes without deciphering complex normalized bridges.
- **Aggregations:** Additive facts (`net_revenue`, `quantity`, `gross_profit`) roll up cleanly across any dimension attribute (e.g., category, customer state, fiscal quarter).

### 3. Slowly Changing Dimension (SCD Type 2) Rationale
- When customers relocate between states (e.g. moving from New York to Texas), an SCD Type 1 model would overwrite the customer's state, improperly attributing prior historical revenue to the new jurisdiction.
- This warehouse implements **SCD Type 2** on `dim_customers` using surrogate keys (`customer_key`), timestamp validity boundaries (`valid_from`, `valid_to`), and an active status flag (`is_current`).
- `fct_orders` joins to `dim_customers` using point-in-time timestamp matching:
  ```sql
  ON fact.customer_id = dim.customer_id
  AND fact.order_timestamp >= dim.valid_from
  AND (fact.order_timestamp < dim.valid_to OR dim.valid_to IS NULL)
  ```
  This guarantees historical reports accurately preserve customer geography at the exact time of order placement.

### 4. Indexing & Partitioning Strategy
- **B-Tree Indexes** are configured on all surrogate keys (`customer_key`, `product_key`, `date_key`) and foreign keys on `fct_orders` to optimize merge and hash joins.
- For high-volume production scale, `fct_orders` is primed for **Range Partitioning by `order_date_key`** (e.g., monthly partitions) to enable partition pruning on temporal range queries.

---

## Repository Structure

```
ecommerce-elt-local/
├── docker-compose.yml           # Multi-service stack: Postgres, Airflow, MinIO, Metabase
├── Dockerfile.airflow           # Airflow 2.9 image extended with dbt-postgres & ingestion libraries
├── .env.example                 # Environment configuration template
├── requirements.txt             # Python project dependencies
├── ingestion/
│   ├── config.yaml              # Volume scale, date ranges, storage settings
│   ├── generate_data.py         # Faker-based synthetic e-commerce data generator (SCD2 aware)
│   ├── load_to_bronze.py        # Lands raw JSON to ./data/bronze/ and MinIO bucket
│   └── load_to_postgres.py      # Batch loads raw JSONB into Postgres 'raw' schema
├── dbt/
│   ├── dbt_project.yml          # Project configuration & schema routing
│   ├── packages.yml             # dbt_utils package dependency
│   ├── profiles.yml.example     # Postgres connection profiles
│   ├── macros/
│   │   └── generate_schema_name.sql # Macro for custom schema generation
│   ├── models/
│   │   ├── sources.yml          # Raw landing source declarations
│   │   ├── staging/             # Silver layer: JSON parsing & type enforcement
│   │   │   ├── stg_customers.sql
│   │   │   ├── stg_products.sql
│   │   │   ├── stg_sessions.sql
│   │   │   ├── stg_orders.sql
│   │   │   ├── stg_order_items.sql
│   │   │   └── staging.yml
│   │   ├── intermediate/        # Silver-Gold bridge: business calculations
│   │   │   ├── int_sessions.sql
│   │   │   ├── int_order_margins.sql
│   │   │   └── intermediate.yml
│   │   └── marts/               # Gold layer: Kimball dimensional star schema
│   │       ├── dim_customers.sql # SCD Type 2 dimension
│   │       ├── dim_products.sql
│   │       ├── dim_date.sql
│   │       ├── fct_orders.sql   # Transactional fact table
│   │       └── marts.yml        # Schema & foreign key tests
│   └── tests/                   # Singular custom assertions
│       ├── assert_net_revenue_positive.sql
│       └── assert_scd2_date_validity.sql
├── airflow/
│   └── dags/
│       └── elt_pipeline_dag.py  # Orchestrates full end-to-end ELT schedule & alerts
├── docs/
│   ├── er_diagram.png           # Visual Kimball ER diagram
│   ├── generate_er_diagram.py   # Script to regenerate ER diagram
│   └── data_dictionary.md       # Comprehensive table and field dictionary
└── README.md
```

---

## Quick Start Guide

### Prerequisites
- [Docker](https://docs.docker.com/engine/install/) (v24+)
- [Docker Compose](https://docs.docker.com/compose/) (v2.20+)
- Python 3.10+ (for optional host-level development)

### 1. Launch Stack with Docker Compose
Clone the repository and run:
```bash
cp .env.example .env
docker compose up -d
```

This starts:
- **PostgreSQL 16**: Port `5432` (`warehouse` and `airflow` databases)
- **Airflow Webserver**: Port `8080` (UI: `admin` / `admin`)
- **Airflow Scheduler**: Background task coordinator
- **MinIO S3**: Port `9000` (API), Port `9001` (Console: `minioadmin` / `minioadmin`)
- **Metabase**: Port `3000` (Local BI dashboard)

### 2. Airflow Orchestration DAG
1. Navigate to `http://localhost:8080` and log in with `admin` / `admin`.
2. Locate the DAG: `ecommerce_elt_pipeline`.
3. Trigger the DAG manually or enable the `@daily` schedule.

**DAG Execution Pipeline:**
```
[land_bronze_data]
        │
        ▼
[load_raw_to_postgres]
        │
        ▼
   [dbt_deps]
        │
        ▼
[dbt_run_staging]
        │
        ▼
[dbt_run_intermediate]
        │
        ▼
 [dbt_run_marts]
        │
        ▼
   [dbt_test] (63 tests)
        │
        ▼
[pipeline_success_audit]
```

### 3. Running Locally (CLI Option)
If developing outside Docker:
```bash
# Set up virtual environment
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 1. Generate synthetic data & land to bronze
python -m ingestion.load_to_bronze

# 2. Load bronze JSON to Postgres
POSTGRES_HOST=localhost python -m ingestion.load_to_postgres

# 3. Execute dbt transformations & tests
cd dbt
POSTGRES_HOST=localhost dbt deps
POSTGRES_HOST=localhost dbt run
POSTGRES_HOST=localhost dbt test
```

---

## dbt Transformation & Testing Layer

The project features a **63-test automated quality assurance suite** covering:
- **Uniqueness & Non-Null:** Validates primary keys and natural identifiers across all staging, intermediate, and marts tables.
- **Referential Integrity:** Enforces foreign key relationships from `fct_orders` to `dim_customers`, `dim_products`, and `dim_date`.
- **Custom Business Rules:**
  - `assert_net_revenue_positive`: Verifies line item revenue cannot be negative after discounts.
  - `assert_scd2_date_validity`: Validates that `valid_to > valid_from` for all historical customer records.

---

## Sample Analytical Queries for BI / SQL Exploration

Connect Metabase or Power BI to PostgreSQL (`host: localhost`, `port: 5432`, `db: warehouse`, `user: postgres`, `password: postgres`).

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
    ROUND((SUM(f.gross_profit) / NULLIF(SUM(f.net_revenue), 0) * 100), 2) AS profit_margin_pct
FROM marts.fct_orders f
JOIN marts.dim_products p ON f.product_key = p.product_key
WHERE f.order_status = 'completed'
GROUP BY p.category
ORDER BY total_net_revenue DESC;
```

### 2. SCD Type 2 Historical Verification: Sales by Customer State at Order Time
```sql
SELECT
    c.state AS customer_state_at_purchase,
    COUNT(DISTINCT f.order_id) AS order_count,
    SUM(f.net_revenue) AS total_revenue
FROM marts.fct_orders f
JOIN marts.dim_customers c ON f.customer_key = c.customer_key
GROUP BY c.state
ORDER BY total_revenue DESC
LIMIT 10;
```

### 3. Digital Marketing Conversion Attribution
```sql
SELECT
    s.traffic_source,
    s.device_type,
    COUNT(s.session_id) AS total_sessions,
    COUNT(s.converted_order_id) AS converted_sessions,
    ROUND((COUNT(s.converted_order_id)::numeric / COUNT(s.session_id) * 100), 2) AS conversion_rate_pct,
    SUM(s.session_attributed_revenue) AS total_revenue
FROM intermediate.int_sessions s
GROUP BY s.traffic_source, s.device_type
ORDER BY total_revenue DESC;
```

---

## License & Author
Built for professional Data & Analytics Engineering portfolios under the MIT License.
