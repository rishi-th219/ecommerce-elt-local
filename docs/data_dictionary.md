# E-Commerce Data Warehouse: Data Dictionary & Modeling Specs

The warehouse is a Kimball star schema built on **Microsoft SQL Server 2025**, modeled
with dbt Core and orchestrated by Apache Airflow. Every model also compiles and
passes its tests on **PostgreSQL 16** through the cross-engine macros in
`dbt/macros/`, so each column below lists both physical types.

---

## 1. Layer Architecture & Grain

```
       [ raw.raw_* ]           NVARCHAR(MAX) JSON landing zone (JSONB on Postgres)
             │                 loaded transactionally by ingestion/load_to_*.py
             ▼
     [ staging.stg_* ]         Materialized tables: JSON parsed once, ANSI types enforced
             │
             ▼
  [ intermediate.int_* ]       Views: sessionization and line-item economics
             │
             ▼
       [ marts.* ]             Tables: Kimball star schema + derived marts
```

**Type mapping across engines**

| Concept | Microsoft SQL Server | PostgreSQL |
|---|---|---|
| JSON landing payload | `NVARCHAR(MAX)` | `JSONB` |
| JSON field access | `JSON_VALUE(payload, '$.key')` | `payload->>'key'` |
| Timestamp | `DATETIME2` | `TIMESTAMP` |
| Boolean flag | `BIT` (1/0) | `BOOLEAN` |
| Decimal measure | `NUMERIC(p,s)` | `NUMERIC(p,s)` |
| Date key conversion | `CONVERT(INT, CONVERT(VARCHAR(8), col, 112))` | `TO_CHAR(col,'YYYYMMDD')::INTEGER` |

**Conventions used throughout the marts layer**

* **Surrogate keys** are MD5 hashes produced by `dbt_utils.generate_surrogate_key`, stored as `VARCHAR`.
* **Unknown members** — every dimension carries a `-1` row (`19000101` for `dim_date`). Facts route unresolved foreign keys to it, so a key lookup failure never produces a `NULL` that would silently drop rows from an inner-joined BI query.
* **Beginning of time** — the first SCD2 version of each customer starts at `1900-01-01` so early-arriving facts always resolve to a dimension version.

---

## 2. Star Schema Dimensions

### 2.1 `marts.dim_customers` (SCD Type 2)

* **Grain:** one row per customer *version*.
* **Purpose:** preserves relocation history so revenue is attributed to the state the customer lived in at the time of purchase, not their state today.
* **Surrogate key:** MD5 of `(customer_id, updated_at)`, deduplicated so a replayed source event cannot collide.

| Column | MSSQL Type | Postgres Type | Key Type | Description |
|---|---|---|---|---|
| `customer_key` | `VARCHAR(32)` | `TEXT` | PK (surrogate) | Unique key for this version of the customer |
| `customer_id` | `VARCHAR(64)` | `VARCHAR(64)` | NK (natural) | Business customer identifier (`CUST-0001`) |
| `first_name` | `VARCHAR(100)` | `VARCHAR(100)` | Attribute | Given name as of this version |
| `last_name` | `VARCHAR(100)` | `VARCHAR(100)` | Attribute | Family name as of this version |
| `email` | `VARCHAR(255)` | `VARCHAR(255)` | Attribute | Contact email as of this version |
| `city` | `VARCHAR(100)` | `VARCHAR(100)` | Attribute | Billing city as of this version |
| `state` | `VARCHAR(50)` | `VARCHAR(50)` | Attribute | US state as of this version — the attribute SCD2 exists to preserve |
| `postal_code` | `VARCHAR(20)` | `VARCHAR(20)` | Attribute | Postal code as of this version |
| `country` | `VARCHAR(50)` | `VARCHAR(50)` | Attribute | Country as of this version |
| `created_at` | `DATETIME2` | `TIMESTAMP` | Metadata | Account signup timestamp; constant across versions |
| `valid_from` | `DATETIME2` | `TIMESTAMP` | SCD2 boundary | Inclusive start of validity. `1900-01-01` for the first version |
| `valid_to` | `DATETIME2` | `TIMESTAMP` | SCD2 boundary | Exclusive end of validity; `NULL` for the current version |
| `effective_updated_at` | `DATETIME2` | `TIMESTAMP` | Lineage | Source `updated_at` that produced this version, retained after `valid_from` anchoring |
| `is_current` | `BIT` | `BOOLEAN` | SCD2 flag | 1/`TRUE` for the version in force today |

### 2.2 `marts.dim_products` (SCD Type 1)

* **Grain:** one row per catalog product.
* **Surrogate key:** MD5 of `product_id`.

| Column | MSSQL Type | Postgres Type | Key Type | Description |
|---|---|---|---|---|
| `product_key` | `VARCHAR(32)` | `TEXT` | PK (surrogate) | Unique key for the product |
| `product_id` | `VARCHAR(64)` | `VARCHAR(64)` | NK (natural) | Product business code (`PROD-001`) |
| `sku` | `VARCHAR(64)` | `VARCHAR(64)` | Natural | Stock keeping unit (`SKU-ELE-001`) |
| `product_name` | `VARCHAR(255)` | `VARCHAR(255)` | Attribute | Merchandising display name |
| `category` | `VARCHAR(100)` | `VARCHAR(100)` | Attribute | Merchandising category |
| `unit_cost` | `NUMERIC(10,2)` | `NUMERIC(10,2)` | Attribute | Acquisition cost per unit |
| `retail_price` | `NUMERIC(10,2)` | `NUMERIC(10,2)` | Attribute | Catalog list price per unit |
| `baseline_margin_pct` | `NUMERIC(10,2)` | `NUMERIC(10,2)` | Metric | `(retail_price - unit_cost) / retail_price * 100`; `NULL` for the Unknown member |
| `created_at` | `DATETIME2` | `TIMESTAMP` | Metadata | Date the product entered the catalog |

### 2.3 `marts.dim_date`

* **Grain:** one row per calendar day, plus the `19000101` Unknown member.
* **Span:** controlled by the `date_spine_start` and `date_spine_days` project variables in `dbt_project.yml` (default: 1460 days from 2023-01-01).
* **Role-playing:** serves as order date on `fct_orders` and session date on `fct_sessions`.

| Column | MSSQL Type | Postgres Type | Key Type | Description |
|---|---|---|---|---|
| `date_key` | `INT` | `INTEGER` | PK | `YYYYMMDD` integer (e.g. `20240315`) |
| `full_date` | `DATE` | `DATE` | Attribute | ISO calendar date |
| `day_of_week` | `INT` | `INTEGER` | Attribute | ISO day index (1 = Monday … 7 = Sunday) |
| `day_name` | `VARCHAR(30)` | `VARCHAR(30)` | Attribute | Weekday name |
| `day_of_month` | `INT` | `INTEGER` | Attribute | Day within the month |
| `month` | `INT` | `INTEGER` | Attribute | Month number (1–12) |
| `month_name` | `VARCHAR(30)` | `VARCHAR(30)` | Attribute | Month name |
| `quarter` | `INT` | `INTEGER` | Attribute | Calendar quarter (1–4) |
| `fiscal_quarter` | `VARCHAR(10)` | `VARCHAR(10)` | Attribute | Quarter label (`Q1`–`Q4`) |
| `year` | `INT` | `INTEGER` | Attribute | Calendar year |
| `is_weekend` | `BIT` | `BOOLEAN` | Flag | 1/`TRUE` for Saturday and Sunday |

---

## 3. Star Schema Facts

### 3.1 `marts.fct_orders` (transaction fact)

* **Grain:** one row per order line item (`order_item_id`).
* **SCD2 join:** a single point-in-time join —
  `order_date >= valid_from AND (order_date < valid_to OR valid_to IS NULL)`.

| Column | MSSQL Type | Postgres Type | Key Type | Description |
|---|---|---|---|---|
| `fct_order_item_key` | `VARCHAR(32)` | `TEXT` | PK (surrogate) | MD5 of `order_item_id` |
| `order_item_id` | `VARCHAR(64)` | `VARCHAR(64)` | Degenerate | Line item identifier (`ITEM-000101`) |
| `order_id` | `VARCHAR(64)` | `VARCHAR(64)` | Degenerate | Parent order header (`ORD-00101`) |
| `customer_key` | `VARCHAR(32)` | `TEXT` | FK | `dim_customers.customer_key` — the version in force at purchase |
| `product_key` | `VARCHAR(32)` | `TEXT` | FK | `dim_products.product_key` |
| `order_date_key` | `INT` | `INTEGER` | FK | `dim_date.date_key` |
| `session_id` | `VARCHAR(64)` | `VARCHAR(64)` | Degenerate | Web session that produced the order; may be `NULL` |
| `order_timestamp` | `DATETIME2` | `TIMESTAMP` | Attribute | Exact purchase timestamp |
| `order_status` | `VARCHAR(50)` | `VARCHAR(50)` | Attribute | `completed`, `cancelled`, `returned`, `processing` |
| `payment_method` | `VARCHAR(50)` | `VARCHAR(50)` | Attribute | `credit_card`, `paypal`, `apple_pay`, `debit_card` |
| `quantity` | `INT` | `INTEGER` | Additive | Units sold on the line |
| `unit_price` | `NUMERIC(10,2)` | `NUMERIC(10,2)` | Non-additive | Price charged per unit |
| `unit_cost` | `NUMERIC(10,2)` | `NUMERIC(10,2)` | Non-additive | Cost per unit at time of sale |
| `gross_amount` | `NUMERIC(12,2)` | `NUMERIC(12,2)` | Additive | `quantity * unit_price` |
| `discount_amount` | `NUMERIC(12,2)` | `NUMERIC(12,2)` | Additive | Discount applied to the line |
| `net_revenue` | `NUMERIC(12,2)` | `NUMERIC(12,2)` | Additive | `gross_amount - discount_amount` |
| `cogs` | `NUMERIC(12,2)` | `NUMERIC(12,2)` | Additive | `quantity * unit_cost` |
| `gross_profit` | `NUMERIC(12,2)` | `NUMERIC(12,2)` | Additive | `net_revenue - cogs` |
| `profit_margin_pct` | `NUMERIC(10,2)` | `NUMERIC(10,2)` | Non-additive | `gross_profit / net_revenue * 100`; recompute from sums when aggregating |

### 3.2 `marts.fct_sessions` (transaction fact)

* **Grain:** one row per web session (`session_id`).
* **Purpose:** connects acquisition channel and on-site engagement to realized revenue, inside the marts schema BI tools already connect to.

| Column | MSSQL Type | Postgres Type | Key Type | Description |
|---|---|---|---|---|
| `session_key` | `VARCHAR(32)` | `TEXT` | PK (surrogate) | MD5 of `session_id` |
| `session_id` | `VARCHAR(64)` | `VARCHAR(64)` | Degenerate | Session identifier (`SESS-000001`) |
| `customer_key` | `VARCHAR(32)` | `TEXT` | FK | `dim_customers.customer_key` at session start |
| `session_date_key` | `INT` | `INTEGER` | FK | `dim_date.date_key` on the session start date |
| `session_start` | `DATETIME2` | `TIMESTAMP` | Attribute | Session start timestamp |
| `session_end` | `DATETIME2` | `TIMESTAMP` | Attribute | Session end timestamp |
| `device_type` | `VARCHAR(50)` | `VARCHAR(50)` | Attribute | `mobile`, `desktop`, `tablet` |
| `traffic_source` | `VARCHAR(100)` | `VARCHAR(100)` | Attribute | `organic_search`, `direct`, `cpc_ad`, `email_campaign`, `social_media`, `affiliate` |
| `duration_seconds` | `INT` | `INTEGER` | Additive | Session length in seconds |
| `duration_minutes` | `NUMERIC(10,2)` | `NUMERIC(10,2)` | Additive | Session length in minutes |
| `page_views_count` | `INT` | `INTEGER` | Additive | Pages viewed in the session |
| `is_converted` | `BIT` | `BOOLEAN` | Flag | Source conversion flag |
| `converted_order_id` | `VARCHAR(64)` | `VARCHAR(64)` | Degenerate | First order placed in the session; `NULL` if none |
| `order_count` | `INT` | `INTEGER` | Additive | Orders placed in the session |
| `session_attributed_revenue` | `NUMERIC(12,2)` | `NUMERIC(12,2)` | Additive | Net revenue attributed to the session |

### 3.3 `marts.fct_daily_sales` (periodic snapshot fact)

* **Grain:** one row per trading day (`date_key`).
* **Purpose:** day-level performance without scanning the line-item grain. Non-trading days are absent by design; join to `dim_date` for a dense series.

| Column | MSSQL Type | Postgres Type | Key Type | Description |
|---|---|---|---|---|
| `date_key` | `INT` | `INTEGER` | PK / FK | `dim_date.date_key`; the grain of the snapshot |
| `order_count` | `INT` | `BIGINT` | Additive | Distinct orders placed |
| `customer_count` | `INT` | `BIGINT` | Semi-additive | Distinct customer versions that ordered (do not sum across days) |
| `order_line_count` | `INT` | `BIGINT` | Additive | Order lines recorded |
| `units_sold` | `INT` | `BIGINT` | Additive | Units sold |
| `gross_amount` | `NUMERIC(14,2)` | `NUMERIC(14,2)` | Additive | Revenue before discounts |
| `discount_amount` | `NUMERIC(14,2)` | `NUMERIC(14,2)` | Additive | Discounts granted |
| `net_revenue` | `NUMERIC(14,2)` | `NUMERIC(14,2)` | Additive | Revenue after discounts |
| `cogs` | `NUMERIC(14,2)` | `NUMERIC(14,2)` | Additive | Cost of goods sold |
| `gross_profit` | `NUMERIC(14,2)` | `NUMERIC(14,2)` | Additive | `net_revenue - cogs` |
| `profit_margin_pct` | `NUMERIC(10,2)` | `NUMERIC(10,2)` | Non-additive | Day margin, recomputed from the day's sums |
| `completed_net_revenue` | `NUMERIC(14,2)` | `NUMERIC(14,2)` | Additive | Net revenue from `completed` lines |
| `returned_net_revenue` | `NUMERIC(14,2)` | `NUMERIC(14,2)` | Additive | Net revenue from `returned` lines |
| `cancelled_net_revenue` | `NUMERIC(14,2)` | `NUMERIC(14,2)` | Additive | Net revenue from `cancelled` lines |

### 3.4 `marts.mart_customer_360` (accumulating summary)

* **Grain:** one row per customer (`customer_id`), joined to their current dimension version.
* **Purpose:** lifetime value plus an RFM quintile segmentation a marketing team can filter on directly.
* **Recency baseline:** measured against the latest order in the warehouse, not `CURRENT_DATE`, so segments do not drift with the query date.

| Column | MSSQL Type | Postgres Type | Key Type | Description |
|---|---|---|---|---|
| `customer_key` | `VARCHAR(32)` | `TEXT` | FK | Current version in `dim_customers` |
| `customer_id` | `VARCHAR(64)` | `VARCHAR(64)` | NK | Customer natural key; the grain |
| `first_name`, `last_name`, `email` | `VARCHAR` | `VARCHAR` | Attribute | Current contact details |
| `city`, `state`, `country` | `VARCHAR` | `VARCHAR` | Attribute | Current geography |
| `customer_since` | `DATETIME2` | `TIMESTAMP` | Attribute | Signup timestamp |
| `lifetime_order_count` | `INT` | `BIGINT` | Measure | Distinct orders ever placed |
| `lifetime_net_revenue` | `NUMERIC(14,2)` | `NUMERIC(14,2)` | Measure | Lifetime net revenue |
| `lifetime_gross_profit` | `NUMERIC(14,2)` | `NUMERIC(14,2)` | Measure | Lifetime gross profit |
| `avg_order_value` | `NUMERIC(14,2)` | `NUMERIC(14,2)` | Measure | Revenue per order; `NULL` if never purchased |
| `returned_order_count` | `INT` | `BIGINT` | Measure | Orders that ended in a return |
| `first_order_at` / `last_order_at` | `DATETIME2` | `TIMESTAMP` | Attribute | Purchase window boundaries |
| `days_since_last_order` | `INT` | `INTEGER` | Measure | Recency in days against the warehouse watermark |
| `session_count` | `INT` | `BIGINT` | Measure | Sessions attributed to the customer |
| `total_page_views` | `INT` | `BIGINT` | Measure | Page views across those sessions |
| `recency_score` / `frequency_score` / `monetary_score` | `INT` | `INTEGER` | Attribute | RFM quintiles 1–5; `0` for never-purchased |
| `rfm_segment` | `VARCHAR` | `TEXT` | Attribute | `Champion`, `Promising`, `Needs Attention`, `At Risk (High Value)`, `Hibernating`, `Never Purchased` |

---

## 4. Intermediate Models

### 4.1 `intermediate.int_sessions`

* **Grain:** one row per session (`session_id`).
* **Note:** orders are aggregated to the session grain *before* the join. Joining order rows directly would fan a multi-order session into duplicate rows and double count sessions, page views and durations.

| Column | Description |
|---|---|
| `session_id`, `customer_id` | Session natural key and its owner |
| `session_start`, `session_end`, `duration_seconds`, `duration_minutes` | Session timing |
| `device_type`, `traffic_source`, `page_views_count` | Engagement context |
| `is_converted` | Source conversion flag |
| `converted_order_id` | Lowest `order_id` placed in the session |
| `order_count` | Orders placed in the session (0 when it did not convert) |
| `session_attributed_revenue` | Sum of net revenue across the session's orders |

### 4.2 `intermediate.int_order_margins`

* **Grain:** one row per order line item (`order_item_id`).
* **Note:** the revenue expression `(quantity * unit_price) - discount_amount` is computed once in the `line_economics` CTE and reused by every downstream measure.

| Column | Description |
|---|---|
| `order_item_id`, `order_id`, `customer_id`, `session_id` | Line and parent identifiers |
| `order_date`, `order_status`, `payment_method` | Order header context |
| `product_id`, `sku`, `product_name`, `category` | Product context |
| `quantity`, `unit_price`, `unit_cost` | Line inputs |
| `gross_amount` | `quantity * unit_price` |
| `discount_amount` | Line discount |
| `net_revenue` | `gross_amount - discount_amount` |
| `cogs` | `quantity * unit_cost` |
| `gross_profit` | `net_revenue - cogs` |
| `profit_margin_pct` | `gross_profit / net_revenue * 100`; `NULL` when net revenue is zero |

---

## 5. Staging Models

All five staging models parse the JSON payload of their raw table into typed
columns and are materialized as **tables**, so `JSON_VALUE` / `->>` runs once per
refresh instead of on every downstream reference.

| Model | Grain | Source |
|---|---|---|
| `staging.stg_customers` | One row per customer *profile event* (not per customer) | `raw.raw_customers` |
| `staging.stg_products` | One row per product | `raw.raw_products` |
| `staging.stg_sessions` | One row per web session | `raw.raw_sessions` |
| `staging.stg_orders` | One row per order header | `raw.raw_orders` |
| `staging.stg_order_items` | One row per order line item | `raw.raw_order_items` |

---

## 6. Data Quality & Test Coverage

The suite runs **213 automated tests** on every `dbt build`, plus `dbt source freshness`
against the raw layer's `_ingested_at` watermark.

| Test category | Targets | Rules enforced |
|---|---|---|
| **Primary key uniqueness** | Every staging, intermediate and marts model | `unique` on all surrogate and natural keys |
| **Completeness** | All keys, attributes and measures | `not_null` across the modeled surface |
| **Referential integrity** | `fct_orders`, `fct_sessions`, `fct_daily_sales`, `mart_customer_360`, staging | `relationships` tests to every dimension and parent table |
| **Domain validity** | `order_status`, `payment_method`, `device_type`, `traffic_source`, `category`, `day_of_week`, RFM scores and segments | `accepted_values` against the closed enumerations |
| **Range validity** | Quantities, prices, discounts, revenues, durations | Project-level `expression_is_true` generic test (`> 0`, `>= 0`, `retail_price >= unit_cost`) |
| **Arithmetic contracts** | `stg_orders`, `stg_order_items` | Model-level `expression_is_true`: `gross_amount - discount_amount = net_revenue`, `(quantity * unit_price) - discount_amount = net_amount` |
| **Source freshness** | `raw.raw_*` | Warn after 24h, error after 48h on `_ingested_at` |
| **Business assertions** | `fct_orders`, `dim_customers` | `assert_net_revenue_positive` — no negative line revenue<br>`assert_scd2_date_validity` — `valid_to > valid_from`<br>`assert_scd2_no_overlapping_versions` — no customer has two versions valid at once<br>`assert_scd2_single_current_version` — exactly one current version per customer<br>`assert_fct_orders_ties_to_source` — fact revenue reconciles to source order headers |

The last assertion is the one that catches a join fan-out: uniqueness tests pass
happily on a duplicated fact grain, but the reconciliation to source order totals
does not.
