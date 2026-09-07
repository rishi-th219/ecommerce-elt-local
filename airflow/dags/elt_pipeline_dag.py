"""
Airflow Orchestration DAG: Local E-Commerce ELT Pipeline

Stages
------
1. Ingestion       Synthetic event generation -> bronze JSON landing zone
2. Loading         Bronze JSON -> `raw` schema of the configured warehouse
3. Transformation  dbt build: staging -> intermediate -> marts, tests interleaved
4. Freshness       dbt source freshness against the raw layer's audit column
5. Alerting        Failure callback and pipeline completion audit

Engine selection
----------------
The warehouse engine is chosen once, from the DBT_TARGET environment variable
(`mssql` by default, `postgres` for the portability path). It picks both the raw
loader module and the dbt `--target`, so the DAG can never load one engine and
transform another. dbt package dependencies are resolved at image build time and
refreshed by `airflow-init`, not on every scheduled run.
"""

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

# ─── Engine selection ──────────────────────────────────────────────────────────
DBT_TARGET = os.getenv("DBT_TARGET", "mssql").strip().lower()
if DBT_TARGET not in ("mssql", "postgres"):
    raise ValueError(
        f"DBT_TARGET must be 'mssql' or 'postgres', got {DBT_TARGET!r}. "
        "Set it in .env and recreate the Airflow containers."
    )

RAW_LOADER_MODULE = "ingestion.load_to_mssql" if DBT_TARGET == "mssql" else "ingestion.load_to_postgres"

PROJECT_HOME = "/opt/airflow"
DBT_DIR = f"{PROJECT_HOME}/dbt"
DBT_FLAGS = f"--profiles-dir {DBT_DIR} --target {DBT_TARGET}"

# Environment shared by every task. Credentials come from the container
# environment (compose injects them from .env), never from the DAG file.
SHARED_ENV = {
    "PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
    "PYTHONPATH": PROJECT_HOME,
    "DBT_TARGET": DBT_TARGET,
    "INGESTION_LOG_LEVEL": os.getenv("INGESTION_LOG_LEVEL", "INFO"),
    "MSSQL_SERVER": os.getenv("MSSQL_SERVER", "mssql"),
    "MSSQL_PORT": os.getenv("MSSQL_PORT", "1433"),
    "MSSQL_USER": os.getenv("MSSQL_USER", "sa"),
    "MSSQL_PASSWORD": os.getenv("MSSQL_PASSWORD", ""),
    "MSSQL_DB": os.getenv("MSSQL_DB", "warehouse"),
    "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "postgres"),
    "POSTGRES_PORT": os.getenv("POSTGRES_PORT", "5432"),
    "POSTGRES_USER": os.getenv("POSTGRES_USER", "postgres"),
    "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", "postgres"),
    "POSTGRES_DB": os.getenv("POSTGRES_DB", "warehouse"),
}

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    # A hung database connection would otherwise hold a worker slot forever.
    "execution_timeout": timedelta(minutes=30),
}


def pipeline_failure_alert(context):
    """Log a structured failure alert.

    `task_instance` is absent from the context for DAG-level failures (for
    example a DAG timeout), so every lookup is defensive -- an AttributeError
    raised inside the callback would mask the real failure.
    """
    task_instance = context.get("task_instance")
    dag_run = context.get("dag_run")

    task_id = getattr(task_instance, "task_id", "<dag-level failure>")
    dag_id = getattr(task_instance, "dag_id", None) or getattr(dag_run, "dag_id", "<unknown dag>")
    log_url = getattr(task_instance, "log_url", "<no task log>")

    logger.error("[ELT ALERT] Pipeline failure in DAG '%s' on task '%s'", dag_id, task_id)
    logger.error("Details: %s", context.get("exception"))
    logger.error("Audit log URL: %s", log_url)


def pipeline_success_summary(**context):
    """Log the completion audit record for the run."""
    logger.info(
        "[ELT SUCCESS] Pipeline finished for run %s at %s (target=%s)",
        context.get("run_id"), context.get("ts"), DBT_TARGET,
    )
    logger.info("Bronze, staging, intermediate and marts layers refreshed and tested.")


with DAG(
    dag_id="ecommerce_elt_pipeline",
    default_args=default_args,
    description=f"End-to-end ELT pipeline for the e-commerce warehouse (target: {DBT_TARGET})",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    on_failure_callback=pipeline_failure_alert,
    tags=["elt", "ecommerce", "dbt", "warehouse", DBT_TARGET],
) as dag:

    # Task 1: generate synthetic events and land them as bronze JSON.
    land_bronze = BashOperator(
        task_id="land_bronze_data",
        bash_command=f"cd {PROJECT_HOME} && python -m ingestion.load_to_bronze",
        env=SHARED_ENV,
        execution_timeout=timedelta(minutes=15),
    )

    # Task 2: load bronze JSON into the `raw` schema of the selected engine.
    load_raw = BashOperator(
        task_id=f"load_raw_to_{DBT_TARGET}",
        bash_command=f"cd {PROJECT_HOME} && python -m {RAW_LOADER_MODULE}",
        env=SHARED_ENV,
        execution_timeout=timedelta(minutes=20),
    )

    # Task 3: fail fast if the raw layer is stale before spending time on models.
    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"cd {DBT_DIR} && dbt source freshness {DBT_FLAGS}",
        env=SHARED_ENV,
        execution_timeout=timedelta(minutes=10),
    )

    # Task 4: one `dbt build` parses the manifest once and runs each model with
    # its tests immediately after it, so a broken model stops its own
    # descendants instead of poisoning the whole marts layer.
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_DIR} && dbt build {DBT_FLAGS}",
        env=SHARED_ENV,
        execution_timeout=timedelta(minutes=45),
    )

    # Task 5: refresh the data catalog served by `dbt docs serve`.
    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"cd {DBT_DIR} && dbt docs generate {DBT_FLAGS}",
        env=SHARED_ENV,
        execution_timeout=timedelta(minutes=10),
    )

    # Task 6: completion audit.
    pipeline_success = PythonOperator(
        task_id="pipeline_success_audit",
        python_callable=pipeline_success_summary,
    )

    land_bronze >> load_raw >> dbt_source_freshness >> dbt_build >> dbt_docs >> pipeline_success
