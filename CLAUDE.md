# Antigravity Agent Instructions — E-Commerce ELT & Jobhunt Projects

## Mission & Context
You are pair-programming with **Rishi Thakur** (`rishiithakur219@gmail.com`), a Senior Data & BI Analyst candidate.
This project is **`ecommerce-elt-local`**, a production-grade modern data warehouse and ELT pipeline built to run 100% free and locally via Docker without cloud trial expiration.

## Immediate Startup Behavior (On Session Start)
When initialized or resuming work:
1. **Do NOT ask Rishi for context or what this project is.** Everything is documented.
2. Read `FEDORA_MIGRATION_HANDOVER.md` and `PROJECT_STUDY_GUIDE.md` for ground truth.
3. Automatically check stack health:
   - Check containers: `docker ps` (ensure MSSQL 2025, Postgres 16, Airflow Webserver, Airflow Scheduler, Metabase, and MinIO are running).
   - Check dbt tests: `docker exec ecommerce-airflow-webserver bash -c "cd /opt/airflow/dbt && dbt test --target mssql --profiles-dir /opt/airflow/dbt"` (verify all 213 tests pass).
4. State the environment health in 2-3 lines and prompt Rishi on the active next milestone:
   - **LinkedIn Storytelling Series**: Review/publish Post 2 (Raw JSON ELT vs Cleaning on Ingest) in `LINKEDIN_POSTS.md`.
   - **Interview Prep**: Run mock interview drills from `PROJECT_STUDY_GUIDE.md` (Kimball modeling, SCD2 window logic, fan-out fixes).
   - **Reporting Layer**: Metabase questions/dashboards and documentation.

## Core Architectural Invariants (Never Violate)
1. **Kimball Star Schema with Surrogate Keys:**
   - Facts: `fct_orders` (transaction line grain), `fct_sessions` (web funnel grain), `fct_daily_sales` (periodic snapshot).
   - Dimensions: `dim_customers` (SCD Type 2 with point-in-time join), `dim_products`, `dim_date`.
   - Kimball Unknown Member (`-1` / `19000101`): Facts never resolve to `NULL` for missing FKs; they resolve to `-1` so inner joins in BI dashboards never drop rows.
2. **SCD Type 2 Historical Accuracy:**
   - Customer moves (e.g. NY to TX) create a new version row in `dim_customers` with `valid_from`, `valid_to`, and `is_current`.
   - `fct_orders` joins on `order_date >= valid_from AND (order_date < valid_to OR valid_to IS NULL)` to guarantee point-in-time revenue attribution.
3. **Sessionization Fan-Out Defense:**
   - `int_sessions` aggregates multiple orders per session *before* joining to web session events to avoid cartesian product row explosion.
4. **Dual-Engine Macro Architecture:**
   - Jinja macros in `dbt/macros/` (`json_extract`, `date_to_key`, `bool_flag`) maintain 100% compatibility between Microsoft SQL Server 2025 and PostgreSQL 16.
5. **Quality Assurance Contract:**
   - The test suite contains **213 automated tests** (schema tests, unique, not_null, relationship integrity, and arithmetic reconciliation). Every change must pass `dbt test`.
6. **Metabase Application DB:**
   - Persistent database is at `/metabase.db/metabase.db.mv.db` inside `ecommerce-metabase`.

## Ground Truth File Index
- `FEDORA_MIGRATION_HANDOVER.md`: Migration runbook, system packages, trap resolution, and restoration instructions.
- `PROJECT_STUDY_GUIDE.md`: 338-line master technical runbook and Kimball interview Q&A.
- `LINKEDIN_POSTS.md`: 5-part storytelling series for personal brand and recruiter inbound (Post 1 published, Posts 2-5 ready).
- `IMPROVEMENT_SUGGESTIONS.md`: 44 architectural improvements and code audit.
- `dbt/models/marts/marts.yml`: Contracts, grain, and test assertions.
- `airflow/dags/elt_pipeline_dag.py`: Orchestration DAG with defensive failure alerting and SLA checks.
