# Local ELT E-Commerce Data Warehouse (Microsoft SQL Server + dbt + Airflow + Power BI)
## End-to-End Architectural Deep-Dive & LinkedIn Storytelling Series

---

# PART 1: The End-to-End Architectural Deep-Dive

## 1. Executive Summary & The "Why"
Most data engineering and business analytics portfolio projects suffer from two fatal flaws:
1. **The Cloud Expiry Clock:** Built on Snowflake/BigQuery/AWS free trials that expire in 14–30 days, leaving broken dashboard links, expired connection strings, and dead infrastructure on a resume.
2. **Cookie-Cutter Pipelines:** Simple CSV loads into a database with basic `SELECT *` aggregations and no dimensional modeling rigor, no orchestration, and zero automated data quality testing.

This project solves both problems:
It replicates the **exact enterprise modern data stack** (ELT pattern, raw bronze landing, dbt transformations, Kimball dimensional modeling, Slowly Changing Dimensions Type 2, automated testing, and Apache Airflow DAG orchestration) using **Microsoft SQL Server 2025** and **100% free, local, open-source tooling** orchestrated seamlessly via Docker Compose.

```
[Python Synthetic Event Generator]
                 │ (Raw JSON files)
                 ▼
[Bronze Landing Zone: Local / MinIO S3] (./data/bronze/)
                 │ (pymssql / pyodbc bulk batch ingestion)
                 ▼
[Microsoft SQL Server 2025: "raw" Schema] (NVARCHAR(MAX) JSON landing + DATETIME2 audit)
                 │
                 ▼
[dbt Core Transformations (dbt-sqlserver): Silver to Gold]
       ├── staging:      parse JSON with JSON_VALUE, enforce ANSI types, rename columns
       ├── intermediate: sessionize user journeys, calculate line margins & COGS
       └── marts:        Kimball Star Schema (dim_customers SCD2, dim_products, dim_date, fct_orders)
                 ▲
                 │ (Scheduled daily run + failure alerting)
[Apache Airflow Orchestration] (Webserver + Scheduler running in Docker)
                 │
                 ▼
[Power BI & Metabase BI Layer] (Direct connection to marts schema on Port 1433 / 3000)
```

---

## 2. Layer-by-Layer Technical Breakdown

### Layer 1: Synthetic Data Generation (`ingestion/generate_data.py`)
- **Library:** `Faker` with fixed seeds (`seed=42`) for reproducibility.
- **Relational Integrity:** Generates 5 interconnected entities:
  1. `customers`: Customer profiles with artificial relocation events (20% of customers experience state moves over a 12-month period to simulate historical updates for SCD2).
  2. `products`: 60 products across 6 real-world categories with wholesale `unit_cost` and retail prices.
  3. `sessions`: 2,000 digital web sessions tracking device types, traffic channels (CPC, organic, social, email), and page views.
  4. `orders`: 1,200 orders tied directly to converted web sessions and customers.
  5. `order_items`: 3,009 line-level items with quantities, discounts, and item gross revenues.

### Layer 2: Bronze Landing & Ingestion (`ingestion/load_to_bronze.py` & `ingestion/load_to_mssql.py`)
- **ELT Philosophy:** Traditional ETL alters and cleans data before database insertion. Modern ELT lands raw data with zero destructive mutation directly into the data warehouse.
- **MSSQL Implementation:**
  - `load_to_bronze.py`: Dumps raw JSON files into `./data/bronze/` and MinIO S3 bucket.
  - `load_to_mssql.py`: Uses `pymssql` to bulk-insert raw JSON documents into SQL Server `raw.raw_*` tables.
  - **Schema Structure:** Each raw table contains:
    ```sql
    CREATE TABLE raw.raw_orders (
        id INT IDENTITY(1,1) PRIMARY KEY,
        payload NVARCHAR(MAX) NOT NULL,
        _ingested_at DATETIME2 NOT NULL DEFAULT GETUTCDATE()
    );
    ```
  - **Why NVARCHAR(MAX) JSON?** It preserves schema evolution flexibility. If the upstream order generator adds a new field tomorrow, ingestion never breaks. MSSQL 2025's native `JSON_VALUE` enables ultra-fast querying directly on stored JSON text.

### Layer 3: Silver Layer Transformation (`dbt/models/staging/` & `intermediate/`)
- **Staging Views (`stg_*`):**
  - Unpacks the JSON documents using SQL Server's native `JSON_VALUE(payload, '$.key')` via a cross-engine macro `{{ json_extract('payload', 'key') }}`.
  - Enforces strict ANSI casting (`VARCHAR`, `NUMERIC(10,2)`, `DATETIME2`, `BIT`).
  - Standardizes column naming conventions and handles boolean conversions cleanly.
- **Intermediate Views (`int_*`):**
  - `int_sessions`: Attributes marketing channels and devices to converted orders, calculating session-attributed conversion revenue.
  - `int_order_margins`: Joins line items with order headers and product wholesale costs to calculate line-item Gross Amount, Net Revenue, COGS, Gross Profit, and Profit Margin % before dimensional key lookup.

### Layer 4: Gold Marts & Kimball Star Schema (`dbt/models/marts/`)
This layer implements a Kimball Star Schema optimized for enterprise BI reporting and self-service analytics:

#### A. `dim_customers` (Slowly Changing Dimension Type 2)
- **The Problem:** In an e-commerce business, customers relocate. If a customer lived in North Dakota in March 2024 and bought $500 of electronics, then moved to California in July 2024, an SCD Type 1 model would overwrite their state to CA. Year-end state tax and regional revenue reports would improperly allocate the March revenue to California!
- **The SCD2 Solution in T-SQL:**
  - Uses `LEAD(updated_at) OVER (PARTITION BY customer_id ORDER BY updated_at ASC)` window functions to establish `valid_from` and `valid_to` timestamps.
  - Generates deterministic surrogate keys using MD5/HASHBYTES: `customer_key = dbt_utils.generate_surrogate_key(['customer_id', 'updated_at'])`.
  - Maintains an `is_current` bit flag (`1` for current, `0` for historical).

#### B. `dim_products` & `dim_date`
- `dim_products`: Catalog dimension with surrogate `product_key`, SKU, category, wholesale cost, retail price, and baseline markup percentage.
- `dim_date`: Conformed calendar dimension leveraging SQL Server 2025's `GENERATE_SERIES(0, 1460)` and `DATEADD(day, value, '2023-01-01')`, generating 1,461 calendar days with ISO day-of-week, day name, fiscal quarters (`Q1`-`Q4`), and weekend flags.

#### C. `fct_orders` (Transactional Fact Table)
- **Grain:** One row per order line item (`order_item_id`).
- **Surrogate Fact Key:** `fct_order_item_key`.
- **The SCD2 Point-in-Time Join:**
  ```sql
  LEFT JOIN dim_customers dc_exact
      ON m.customer_id = dc_exact.customer_id
      AND m.order_date >= dc_exact.valid_from
      AND (m.order_date < dc_exact.valid_to OR dc_exact.valid_to IS NULL)
  ```
  This guarantees that every purchase in `fct_orders` points to the historical version of the customer active at the exact moment of sale.

### Layer 5: Automated Testing & Data Contracts (`dbt/tests/`)
A total of **63 automated tests** run on every pipeline execution:
1. **Primary & Surrogate Key Uniqueness:** Every dimension and fact surrogate key is tested for `unique` and `not_null`.
2. **Referential Integrity:** Enforces foreign key relationships (`relationships` test) from `fct_orders` to `dim_customers`, `dim_products`, and `dim_date`.
3. **Singular Custom Assertions:**
   - `assert_net_revenue_positive.sql`: Asserts `net_revenue >= 0` across all line items.
   - `assert_scd2_date_validity.sql`: Asserts `valid_to > valid_from` whenever `valid_to IS NOT NULL`.

### Layer 6: Airflow Orchestration (`airflow/dags/elt_pipeline_dag.py`)
- Scheduled daily (`@daily`) in Docker using `LocalExecutor`.
- The linear dependency graph enforces strict ordering:
  `land_bronze_data` ➔ `load_raw_to_mssql` ➔ `dbt_deps` ➔ `dbt_run_staging` ➔ `dbt_run_intermediate` ➔ `dbt_run_marts` ➔ `dbt_test` ➔ `pipeline_success_audit`.
- Includes `on_failure_callback` alerting hook to log error traces and audit URLs upon task failures.

### Layer 7: BI & Reporting Layer (Power BI / Metabase)
- Connects directly to Microsoft SQL Server 2025 on port `1433` (database `warehouse`, schema `marts`).
- Instant drill-down across:
  - Gross Margin by Product Category
  - Historical Revenue Attribution by Customer State (SCD2 verified)
  - Marketing Acquisition Channel ROI (Session-to-Order Conversion Rate)

---

## 3. How to Speak About This in an Amazon BA / Data Engineering Interview

When interviewers ask: *"Tell me about a data warehouse or pipeline you designed from scratch,"* use this structured talk track:

> *"I engineered an end-to-end local ELT data warehouse built on Microsoft SQL Server 2025 that mirrors enterprise production architectures. I generated referentially consistent synthetic order and event streams, landed raw JSON documents into SQL Server using NVARCHAR(MAX) landing tables with audit metadata, and orchestrated multi-tier transformations using dbt Core and Apache Airflow.*
>
> *The core modeling challenge was handling customer demographic and geographic mobility over time. Instead of an SCD Type 1 overwrite—which corrupts historical state-level revenue attribution—I modeled `dim_customers` as a Kimball Slowly Changing Dimension Type 2 with surrogate keys and validity windows. In the fact table (`fct_orders`), I implemented point-in-time joins matching the transaction timestamp to the customer's active location at the date of purchase.*
>
> *To guarantee data reliability, I implemented a 63-test automated quality assurance suite validating primary keys, foreign key referential integrity across all dimensional boundaries, and custom business logic before publishing the gold marts to Power BI."*

---

# PART 2: The LinkedIn 5-Part Storytelling Series

Here is your 5-part LinkedIn post series designed to showcase your engineering depth, architectural reasoning, and technical storytelling. Post them 1–2 days apart.

---

## 📱 Post 1: The Hook & The Problem
**Goal:** Hook recruiters, data leads, and engineering managers by highlighting enterprise MS SQL Server and solving the "cloud trial expiry" problem.

```text
Why do 90% of data portfolio projects die within 30 days? 💀

Because they're built on cloud free trials:
- Day 1: Set up Snowflake & Fivetran
- Day 14: Trial expires, pipeline breaks
- Day 30: Links on the resume lead to 404s and dead dashboards

When designing my latest data warehouse project, I set an ambitious constraint:
"Build the exact same production-grade ELT stack, Kimball star schema, and orchestration pipeline—using enterprise Microsoft SQL Server and 100% free, local, open-source tools."

No cloud billing. No trial expiry clock. Fully reproducible with a single `docker compose up`.

Here is the stack I built:
🔹 Database: Microsoft SQL Server 2025 (in Docker)
🔹 Ingestion: Python + Faker (generating referentially consistent e-commerce transactions)
🔹 Storage: Raw JSON landing zone (Bronze layer)
🔹 Transformation: dbt Core + dbt-sqlserver (Staging ➔ Intermediate ➔ Kimball Marts)
🔹 Orchestration: Apache Airflow 2.9 (LocalExecutor, daily DAGs, failure callbacks)
🔹 BI Layer: Power BI & Metabase (Direct connection to gold marts)

In this 5-part series, I’m breaking down the architecture, T-SQL JSON parsing, SCD Type 2 modeling challenges, and how I enforced 63 automated data quality tests.

Part 2 tomorrow: Why landing raw data as JSON in SQL Server beats pre-cleaning it.

Repo link in the comments! 👇

#DataEngineering #AnalyticsEngineering #SQLServer #dbt #Airflow #DataModeling #PowerBI
```

---

## 📱 Post 2: The ELT Ingestion & Bronze Layer in Microsoft SQL Server
**Goal:** Explain why ELT and JSON land-first architecture is standard in modern data platforms.

```text
ETL is legacy. ELT is how modern data platforms scale. 🚀 (Part 2/5)

In traditional ETL pipelines, data engineers spent weeks writing custom extraction scripts that filtered, transformed, and casted columns *before* inserting them into the warehouse.

The problem?
The moment an upstream service added a new attribute or changed a data type, the ingestion job crashed. Worse: uncaptured raw data was lost forever.

For my local e-commerce data warehouse, I used the modern ELT pattern backed by Microsoft SQL Server 2025:

1️⃣ Step 1: Ingestion
A Python engine generates synthetic e-commerce events (customers, products, digital sessions, and orders) and lands them as raw JSON files into a local Bronze landing zone.

2️⃣ Step 2: Zero-Loss Loading
Using pymssql batch loaders, raw documents are loaded directly into SQL Server under a dedicated `raw` schema:
```sql
CREATE TABLE raw.raw_orders (
    id INT IDENTITY(1,1) PRIMARY KEY,
    payload NVARCHAR(MAX) NOT NULL,
    _ingested_at DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);
```

Why NVARCHAR(MAX) JSON?
- Zero data loss: If upstream payloads evolve, ingestion never breaks.
- Replayability: If business logic changes, we re-parse history without re-ingesting.
- Native T-SQL efficiency: SQL Server parses JSON with high-performance `JSON_VALUE()` functions.

3️⃣ Step 3: dbt Takes Over
In our dbt staging layer (`staging.stg_*`), we unpack the JSON documents using `JSON_VALUE`, enforce strict ANSI data types, standardize column names, and enforce schema contracts.

Next up in Part 3: The hardest part of dimensional modeling—handling customer relocation using Kimball SCD Type 2.

Thoughts on landing JSON vs typed staging tables? Drop your take below! 👇

#DataEngineering #dbt #MSSQL #SQL #DataWarehouse #ETL
```

---

## 📱 Post 3: The Kimball Star Schema & SCD Type 2 Deep-Dive
**Goal:** The technical centerpiece! Demonstrates deep knowledge of Kimball dimensional modeling and historical tracking in SQL Server.

```text
What happens to your sales reports when a customer moves to another state? 📦 (Part 3/5)

Imagine this scenario:
In March 2024, John lived in North Dakota and made a $500 purchase.
In July 2024, John moved to California.

If your customer dimension uses an SCD Type 1 (overwrite) strategy:
❌ John's state is overwritten to "CA".
❌ Year-end state tax and regional sales dashboards will misattribute John's March purchase to California!
❌ Historical revenue reporting is corrupted.

To solve this in my local e-commerce warehouse, I modeled `dim_customers` as a Kimball Slowly Changing Dimension (SCD Type 2) in Microsoft SQL Server:

🛠️ How it works:
1. When John moves, a new row is inserted rather than overwriting the old one.
2. Window functions (`LEAD(updated_at) OVER (...)`) establish `valid_from` and `valid_to` timestamps.
3. Each record receives a deterministic surrogate key:
   `customer_key = dbt_utils.generate_surrogate_key(['customer_id', 'updated_at'])`
4. An `is_current` bit flag designates the active profile.

🔗 The Fact Table Join (`fct_orders`):
Instead of joining on `customer_id`, the transactional fact table performs a point-in-time join:
```sql
LEFT JOIN dim_customers dc
    ON orders.customer_id = dc.customer_id
    AND orders.order_date >= dc.valid_from
    AND (orders.order_date < dc.valid_to OR dc.valid_to IS NULL)
```

Now, John's March order links to the North Dakota surrogate key (`ND`), while his September order links to the California surrogate key (`CA`).

Historical truth is preserved. 🎯

Part 4 tomorrow: How I automated 63 tests and orchestrated the entire DAG with Apache Airflow.

#Kimball #DataModeling #SQL #SCD2 #dbtCore #SQLServer #Analytics
```

---

## 📱 Post 4: Orchestration & 63 Automated Data Quality Tests
**Goal:** Demonstrate engineering discipline, automated testing, CI/CD, and orchestration.

```text
"If your data warehouse doesn't test itself, your stakeholders will test it for you." ⚠️ (Part 4/5)

Nothing damages trust faster than an executive dashboard displaying negative revenue or duplicate transaction keys.

In my local e-commerce data warehouse, data quality isn't an afterthought—it’s an automated contract enforced at every layer by dbt Core and Apache Airflow.

Every time the pipeline executes, **63 automated tests** run against Microsoft SQL Server:

🛡️ 1. Uniqueness & Nullability:
- Every surrogate key (`customer_key`, `product_key`, `date_key`, `fct_order_item_key`) is verified `unique` and `not_null`.

🛡️ 2. Referential Integrity:
- Foreign key relationship tests verify that 100% of orders in `fct_orders` have matching dimension records in `dim_customers`, `dim_products`, and `dim_date`. Zero orphan transactions.

🛡️ 3. Custom Business Logic Assertions:
- `assert_net_revenue_positive`: Ensures discounts never exceed gross sales.
- `assert_scd2_date_validity`: Ensures `valid_to > valid_from` for all historical customer timeline rows.

⚙️ Orchestration with Apache Airflow:
The entire pipeline runs on an Airflow DAG (`ecommerce_elt_pipeline`) with automated dependencies:
1. `land_bronze_data`
2. `load_raw_to_mssql`
3. `dbt_deps`
4. `dbt_run_staging`
5. `dbt_run_intermediate`
6. `dbt_run_marts`
7. `dbt_test`
8. `pipeline_success_audit`

If a single test fails, the pipeline halts immediately and triggers an automated failure callback with execution logs.

Part 5 tomorrow: The final BI reporting dashboard in Power BI & Metabase and key takeaways!

#ApacheAirflow #DataQuality #dbt #DataOps #SQLServer #AnalyticsEngineering
```

---

## 📱 Post 5: The Analytics Payoff & Project Showcase
**Goal:** Wrap up the story, present visual proof, provide GitHub repo link, and invite engagement.

```text
From raw JSON to executive decision-making. 📊 (Part 5/5)

Over the past week, I broke down how I built a production-grade local ELT Data Warehouse using:
- Microsoft SQL Server 2025
- dbt Core (dbt-sqlserver)
- Apache Airflow 2.9
- Power BI & Metabase
- Docker Compose

Today, the final piece: The Gold Reporting Layer.

Because we modeled our marts layer into a clean Kimball Star Schema, business users can slice financial metrics across any dimension without writing 10-table JOIN queries:

📈 What we unlocked:
1. **Profit Margins by Category:** Real-time visibility into Gross Revenue, Discounts, Cost of Goods Sold (COGS), and Margin % across electronics, apparel, and home goods.
2. **True Historical Attribution:** Regional sales performance that accurately reflects customer relocation via SCD Type 2 dimensions.
3. **Marketing Channel ROI:** Direct attribution connecting web session traffic sources (CPC, organic, social) to net conversion revenue.

💡 Key Project Takeaways:
- You don't need an enterprise cloud budget to build and demonstrate production-grade data engineering.
- Microsoft SQL Server + dbt Core is a lethal, battle-tested combination for enterprise analytics.
- Automated testing (63 tests) is what turns a hobby script into a production data warehouse.

The entire project is containerized, documented, and open-source on GitHub:
👉 https://github.com/rishi-th219/ecommerce-elt-local

Check out the repo, spin it up with `docker compose up`, and let me know what you think! 🚀

#DataEngineering #PortfolioProject #BusinessIntelligence #SQLServer #PowerBI #dbt #OpenSource #DataCareers
```
