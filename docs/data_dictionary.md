# E-Commerce Data Warehouse: Data Dictionary & Modeling Specs

## 1. Architectural Architecture & Modeling Grain

This data warehouse implements a Kimball Star Schema on PostgreSQL orchestrated via Apache Airflow and modeled using dbt Core.

```
       [ raw.raw_* ]  (JSONB Ingestion Landing Zone)
             │
             ▼
     [ staging.stg_* ]  (Parsed Views, Type Enforcement)
             │
             ▼
  [ intermediate.int_* ] (Sessionization & Line Margins)
             │
             ▼
       [ marts.* ]     (Gold Kimball Star Schema)
```

---

## 2. Kimball Star Schema Entities

### 2.1 `marts.dim_customers` (SCD Type 2)
* **Grain:** One row per customer version across time.
* **Purpose:** Preserves demographic and state relocation history so historical sales reflect the customer's location at the time of purchase.
* **Surrogate Key Generation:** MD5 hash of `(customer_id, valid_from)`.

| Column | Data Type | Key Type | Description |
|---|---|---|---|
| `customer_key` | `VARCHAR(32)` | PK (Surrogate) | Unique hash for this version of the customer record |
| `customer_id` | `VARCHAR(64)` | NK (Natural) | Business customer identifier (`CUST-0001`) |
| `first_name` | `VARCHAR(100)` | Attribute | Customer's first name |
| `last_name` | `VARCHAR(100)` | Attribute | Customer's last name |
| `email` | `VARCHAR(255)` | Attribute | Contact email address |
| `city` | `VARCHAR(100)` | Attribute | City of residence |
| `state` | `VARCHAR(50)` | Attribute | State/Province (e.g., `NY`, `CA`, `TX`) |
| `postal_code` | `VARCHAR(20)` | Attribute | Postal/ZIP code |
| `country` | `VARCHAR(50)` | Attribute | Country of residence |
| `created_at` | `TIMESTAMP` | Metadata | Initial account creation timestamp |
| `valid_from` | `TIMESTAMP` | SCD2 Boundary | Start timestamp when this address/profile became active |
| `valid_to` | `TIMESTAMP` | SCD2 Boundary | End timestamp when superseded by an update (`NULL` if current) |
| `is_current` | `BOOLEAN` | SCD2 Flag | `TRUE` if active version, `FALSE` if historical |

---

### 2.2 `marts.dim_products`
* **Grain:** One row per product in the catalog.
* **Purpose:** Product hierarchy, category slicing, cost, and baseline margins.
* **Surrogate Key Generation:** MD5 hash of `product_id`.

| Column | Data Type | Key Type | Description |
|---|---|---|---|
| `product_key` | `VARCHAR(32)` | PK (Surrogate) | Unique hash for the product |
| `product_id` | `VARCHAR(64)` | NK (Natural) | Product business code (`PROD-001`) |
| `sku` | `VARCHAR(64)` | Natural | Stock Keeping Unit (`SKU-ELE-001`) |
| `product_name` | `VARCHAR(255)` | Attribute | Full descriptive product title |
| `category` | `VARCHAR(100)` | Attribute | Primary product department/category |
| `unit_cost` | `NUMERIC(10,2)` | Measure | Wholesale manufacturing / acquisition cost |
| `retail_price` | `NUMERIC(10,2)` | Measure | Standard catalog retail price |
| `baseline_margin_pct` | `NUMERIC(5,2)` | Metric | Catalog markup percentage: `(retail_price - unit_cost)/retail_price * 100` |
| `created_at` | `TIMESTAMP` | Metadata | Timestamp product was added to catalog |

---

### 2.3 `marts.dim_date`
* **Grain:** One row per calendar day.
* **Purpose:** Standardized temporal slicing, ISO calendar weeks, and fiscal quarters.

| Column | Data Type | Key Type | Description |
|---|---|---|---|
| `date_key` | `INTEGER` | PK (Natural) | Temporal key in format `YYYYMMDD` (e.g., `20240315`) |
| `full_date` | `DATE` | Attribute | Standard ISO date (`2024-03-15`) |
| `day_of_week` | `INTEGER` | Attribute | ISO day index (1 = Monday, 7 = Sunday) |
| `day_name` | `VARCHAR(15)` | Attribute | Day name (`Monday`, `Tuesday`, etc.) |
| `day_of_month` | `INTEGER` | Attribute | Day of the month (1 to 31) |
| `month` | `INTEGER` | Attribute | Calendar month number (1 to 12) |
| `month_name` | `VARCHAR(15)` | Attribute | Full month name (`January`, `February`, etc.) |
| `quarter` | `INTEGER` | Attribute | Calendar quarter (1 to 4) |
| `fiscal_quarter` | `VARCHAR(2)` | Attribute | Formatted fiscal quarter (`Q1`, `Q2`, `Q3`, `Q4`) |
| `year` | `INTEGER` | Attribute | Calendar year (`2024`) |
| `is_weekend` | `BOOLEAN` | Flag | `TRUE` if Saturday or Sunday |

---

### 2.4 `marts.fct_orders`
* **Grain:** One row per order line item (`order_item_id`).
* **Purpose:** Atomic transactional revenue, volume, discounting, cost of goods sold (COGS), and profit margin analytics.
* **SCD2 Join Logic:** Joins `dim_customers` on `customer_id` where `order_timestamp >= valid_from AND (order_timestamp < valid_to OR valid_to IS NULL)`.

| Column | Data Type | Key Type | Description |
|---|---|---|---|
| `fct_order_item_key` | `VARCHAR(32)` | PK (Surrogate) | MD5 hash of `order_item_id` |
| `order_item_id` | `VARCHAR(64)` | NK | Line item identifier (`ITEM-000101-1`) |
| `order_id` | `VARCHAR(64)` | Degenerate FK | Parent order header identifier (`ORD-000101`) |
| `customer_key` | `VARCHAR(32)` | FK | Reference to `dim_customers.customer_key` (active version at sale) |
| `product_key` | `VARCHAR(32)` | FK | Reference to `dim_products.product_key` |
| `order_date_key` | `INTEGER` | FK | Reference to `dim_date.date_key` |
| `session_id` | `VARCHAR(64)` | Degenerate FK | Reference to web clickstream session that yielded purchase |
| `order_timestamp` | `TIMESTAMP` | Attribute | Exact purchase timestamp |
| `order_status` | `VARCHAR(50)` | Attribute | Order lifecycle status (`completed`, `cancelled`, `returned`, `processing`) |
| `payment_method` | `VARCHAR(50)` | Attribute | Tender type (`credit_card`, `paypal`, `apple_pay`, `debit_card`) |
| `quantity` | `INTEGER` | Additive Fact | Quantity units purchased |
| `unit_price` | `NUMERIC(10,2)` | Non-additive | Selling price per unit at time of sale |
| `unit_cost` | `NUMERIC(10,2)` | Non-additive | Wholesale cost per unit at time of sale |
| `gross_amount` | `NUMERIC(12,2)` | Additive Fact | Pre-discount total: `quantity * unit_price` |
| `discount_amount` | `NUMERIC(12,2)` | Additive Fact | Promotional discount applied |
| `net_revenue` | `NUMERIC(12,2)` | Additive Fact | Realized revenue: `gross_amount - discount_amount` |
| `cogs` | `NUMERIC(12,2)` | Additive Fact | Cost of goods sold: `quantity * unit_cost` |
| `gross_profit` | `NUMERIC(12,2)` | Additive Fact | Contribution margin: `net_revenue - cogs` |
| `profit_margin_pct` | `NUMERIC(5,2)` | Semi-additive | Line margin percentage: `(net_revenue - cogs)/net_revenue * 100` |

---

## 3. Intermediate Transformation Models

### 3.1 `intermediate.int_sessions`
* **Grain:** One row per digital session (`session_id`).
* **Attributes:** `duration_seconds`, `duration_minutes`, `device_type`, `traffic_source`, `page_views_count`, `is_converted`, `converted_order_id`, `session_attributed_revenue`.

### 3.2 `intermediate.int_order_margins`
* **Grain:** One row per order line item.
* **Attributes:** Pre-computes financial measures (gross revenue, discounts, net revenue, COGS, gross margin) before dimension key lookup.

---

## 4. Data Quality & Test Coverage Summary

| Test Category | Target Tables | Rules Enforced |
|---|---|---|
| **Primary Key Uniqueness** | All staging, intermediate, and marts models | `unique` on all surrogate and natural keys |
| **Not Null Completeness** | All PKs, FKs, and measures | `not_null` on IDs, keys, prices, and revenues |
| **Referential Integrity** | `fct_orders` | FK checks against `dim_customers`, `dim_products`, `dim_date` |
| **Business Logic Assertions** | `fct_orders`, `dim_customers` | `assert_net_revenue_positive` (no negative line revenue)<br>`assert_scd2_date_validity` (`valid_to > valid_from`) |
