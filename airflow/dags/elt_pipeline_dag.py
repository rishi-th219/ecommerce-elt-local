"""
Airflow Orchestration DAG: Local E-Commerce ELT Pipeline
Wires together:
1. Ingestion: Synthetic data generation -> Bronze JSON landing
2. Loading: Bronze JSON -> Postgres 'raw' schema
3. Transformation: dbt deps -> dbt run (staging -> intermediate -> marts)
4. Quality Assurance: dbt test (schema constraints, referential integrity, SCD2 validity)
5. Alerting: Failure callback & pipeline completion notification
"""

from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Default task arguments
default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

def pipeline_failure_alert(context):
    """Callback function triggered on task or pipeline failure."""
    task_id = context.get("task_instance").task_id
    dag_id = context.get("task_instance").dag_id
    log_url = context.get("task_instance").log_url
    error_msg = context.get("exception")

    logging.error(f"🚨 [ELT ALERT] Pipeline Failure in DAG '{dag_id}' on Task '{task_id}'!")
    logging.error(f"Details: {error_msg}")
    logging.error(f"Audit Log URL: {log_url}")

def pipeline_success_summary(**context):
    """Logs completion audit metrics for the ELT run."""
    run_id = context.get("run_id")
    execution_date = context.get("ts")
    logging.info(f"✅ [ELT SUCCESS] Pipeline finished successfully for Run: {run_id} at {execution_date}")
    logging.info("All layers (Bronze, Silver, Gold Marts) refreshed and tested.")

with DAG(
    dag_id="ecommerce_elt_pipeline",
    default_args=default_args,
    description="End-to-end ELT pipeline for E-Commerce Data Warehouse (Local Postgres + dbt)",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    on_failure_callback=pipeline_failure_alert,
    tags=["elt", "ecommerce", "dbt", "warehouse"],
) as dag:

    # Task 1: Land data to bronze layer
    land_bronze = BashOperator(
        task_id="land_bronze_data",
        bash_command="cd /opt/airflow && python -m ingestion.load_to_bronze",
        env={
            "PYTHONPATH": "/opt/airflow",
        },
    )

    # Task 2: Load bronze JSON to Postgres 'raw' schema
    load_raw_postgres = BashOperator(
        task_id="load_raw_to_postgres",
        bash_command="cd /opt/airflow && python -m ingestion.load_to_postgres",
        env={
            "PYTHONPATH": "/opt/airflow",
            "POSTGRES_HOST": "postgres",
            "POSTGRES_PORT": "5432",
            "POSTGRES_USER": "postgres",
            "POSTGRES_PASSWORD": "postgres",
            "POSTGRES_DB": "warehouse",
        },
    )

    # Task 3: Install dbt package dependencies
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="cd /opt/airflow/dbt && dbt deps --profiles-dir /opt/airflow/dbt",
        env={
            "POSTGRES_HOST": "postgres",
            "POSTGRES_PORT": "5432",
            "POSTGRES_USER": "postgres",
            "POSTGRES_PASSWORD": "postgres",
            "POSTGRES_DB": "warehouse",
        },
    )

    # Task 4: Run dbt staging layer
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command="cd /opt/airflow/dbt && dbt run --select staging --profiles-dir /opt/airflow/dbt",
        env={
            "POSTGRES_HOST": "postgres",
            "POSTGRES_PORT": "5432",
            "POSTGRES_USER": "postgres",
            "POSTGRES_PASSWORD": "postgres",
            "POSTGRES_DB": "warehouse",
        },
    )

    # Task 5: Run dbt intermediate layer
    dbt_run_intermediate = BashOperator(
        task_id="dbt_run_intermediate",
        bash_command="cd /opt/airflow/dbt && dbt run --select intermediate --profiles-dir /opt/airflow/dbt",
        env={
            "POSTGRES_HOST": "postgres",
            "POSTGRES_PORT": "5432",
            "POSTGRES_USER": "postgres",
            "POSTGRES_PASSWORD": "postgres",
            "POSTGRES_DB": "warehouse",
        },
    )

    # Task 6: Run dbt marts (star schema & SCD2)
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command="cd /opt/airflow/dbt && dbt run --select marts --profiles-dir /opt/airflow/dbt",
        env={
            "POSTGRES_HOST": "postgres",
            "POSTGRES_PORT": "5432",
            "POSTGRES_USER": "postgres",
            "POSTGRES_PASSWORD": "postgres",
            "POSTGRES_DB": "warehouse",
        },
    )

    # Task 7: Run all dbt data quality & referential integrity tests
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && dbt test --profiles-dir /opt/airflow/dbt",
        env={
            "POSTGRES_HOST": "postgres",
            "POSTGRES_PORT": "5432",
            "POSTGRES_USER": "postgres",
            "POSTGRES_PASSWORD": "postgres",
            "POSTGRES_DB": "warehouse",
        },
    )

    # Task 8: Pipeline audit & success notification
    pipeline_success = PythonOperator(
        task_id="pipeline_success_audit",
        python_callable=pipeline_success_summary,
        provide_context=True,
    )

    # Task dependency graph
    land_bronze >> load_raw_postgres >> dbt_deps >> dbt_run_staging >> dbt_run_intermediate >> dbt_run_marts >> dbt_test >> pipeline_success
