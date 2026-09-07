"""
Postgres Raw Loader

Reads raw JSON artifacts from the bronze landing zone and inserts them into the
Postgres `raw` schema as JSONB payloads with ingestion audit metadata.

The refresh of every entity happens inside a single transaction: tables are
created if missing, TRUNCATEd, repopulated, and committed once at the end, so a
failure part-way through leaves the previous raw layer untouched.
"""

import json
import os

import psycopg2
import yaml
from psycopg2.extras import execute_values

from ingestion.logging_config import get_logger

logger = get_logger(__name__)

ENTITIES = ["customers", "products", "sessions", "orders", "order_items"]
INSERT_PAGE_SIZE = 1000


def _connection_settings(config=None):
    """Resolve connection settings: environment first (Docker/Airflow), then config.yaml."""
    cfg = (config or {}).get("database", {})

    host = os.getenv("POSTGRES_HOST") or cfg.get("host", "localhost")
    # Running on the host, the compose service name will not resolve.
    if host == "postgres" and not os.path.exists("/.dockerenv"):
        host = "localhost"

    return {
        "host": host,
        "port": int(os.getenv("POSTGRES_PORT", cfg.get("port", 5432))),
        "user": os.getenv("POSTGRES_USER") or cfg.get("user", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD") or cfg.get("password", "postgres"),
        "dbname": os.getenv("POSTGRES_DB") or cfg.get("dbname", "warehouse"),
    }


def get_db_connection(config=None):
    settings = _connection_settings(config)
    logger.debug(
        "Connecting to Postgres %s:%s/%s as %s",
        settings["host"], settings["port"], settings["dbname"], settings["user"],
    )
    return psycopg2.connect(**settings)


def load_bronze_to_postgres(bronze_dir=None, config_path="ingestion/config.yaml"):
    config = {}
    if os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}

    local_dir = bronze_dir or config.get("storage", {}).get("local_bronze_dir", "data/bronze")
    raw_schema = config.get("database", {}).get("raw_schema", "raw")

    conn = get_db_connection(config)
    cursor = conn.cursor()
    loaded_counts = {}

    try:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {raw_schema};")

        for entity in ENTITIES:
            file_path = os.path.join(local_dir, f"{entity}.json")
            if not os.path.exists(file_path):
                logger.warning("Bronze file not found, skipping entity: %s", file_path)
                continue

            with open(file_path, "r", encoding="utf-8") as f:
                records = json.load(f)

            table_name = f"{raw_schema}.raw_{entity}"

            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    payload JSONB NOT NULL,
                    _ingested_at TIMESTAMP NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC')
                );
                TRUNCATE TABLE {table_name};
                """
            )

            insert_query = f"INSERT INTO {table_name} (payload) VALUES %s"
            payloads = [(json.dumps(record),) for record in records]
            execute_values(cursor, insert_query, payloads, page_size=INSERT_PAGE_SIZE)

            loaded_counts[entity] = len(records)
            logger.info("Staged %s records for %s", len(records), table_name)

        # Single commit: either every raw table is refreshed, or none of them are.
        conn.commit()
        logger.info(
            "Postgres raw load committed: %s",
            ", ".join(f"{k}={v}" for k, v in loaded_counts.items()) or "no entities loaded",
        )
        return loaded_counts
    except Exception:
        conn.rollback()
        logger.exception("Postgres raw load failed and was rolled back; previous data is intact")
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    load_bronze_to_postgres()
