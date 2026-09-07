"""
MSSQL Raw Loader

Reads raw JSON artifacts from the bronze landing zone and inserts them into the
Microsoft SQL Server `raw` schema as NVARCHAR(MAX) payloads with ingestion audit
metadata.

Load semantics
--------------
The refresh of every entity happens inside a single transaction: tables are
created if missing, TRUNCATEd, repopulated, and only then committed. An earlier
version committed a DROP TABLE before inserting, so a failure mid-insert left the
raw layer permanently empty. Nothing is destroyed here until the replacement rows
are safely staged.
"""

import json
import os

import pymssql
import yaml

from ingestion.logging_config import get_logger

logger = get_logger(__name__)

ENTITIES = ["customers", "products", "sessions", "orders", "order_items"]
INSERT_BATCH_SIZE = 1000


def _connection_settings(config=None):
    """Resolve connection settings: environment first, then config.yaml, then defaults.

    The password has no hardcoded fallback. In production these values would be
    injected from a secrets manager; locally they come from .env.
    """
    cfg = (config or {}).get("mssql", {})

    password = os.getenv("MSSQL_PASSWORD") or cfg.get("password")
    if not password:
        raise RuntimeError(
            "MSSQL password is not configured. Set MSSQL_PASSWORD in the "
            "environment (see .env.example) before running the loader."
        )

    server = os.getenv("MSSQL_SERVER", cfg.get("server", "localhost"))
    # Inside Docker the service is reachable by its compose service name.
    if server == "localhost" and os.path.exists("/.dockerenv"):
        server = cfg.get("docker_server", server)

    return {
        "server": server,
        "port": int(os.getenv("MSSQL_PORT", cfg.get("port", 1433))),
        "user": os.getenv("MSSQL_USER", cfg.get("user", "sa")),
        "password": password,
        "database": os.getenv("MSSQL_DB", cfg.get("database", "warehouse")),
    }


def ensure_database(config=None):
    """Create the warehouse database if the SQL Server instance does not have it.

    Unlike Postgres, the SQL Server image has no init-script hook, so a fresh
    container starts with system databases only.
    """
    settings = _connection_settings(config)
    target_db = settings.pop("database")

    conn = pymssql.connect(database="master", autocommit=True, **settings)
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "IF DB_ID(%s) IS NULL EXEC('CREATE DATABASE [' + %s + ']');",
                (target_db, target_db),
            )
        logger.info("Verified database '%s' exists on %s", target_db, settings["server"])
    finally:
        conn.close()


def get_mssql_connection(config=None):
    settings = _connection_settings(config)
    logger.debug(
        "Connecting to MSSQL %s:%s/%s as %s",
        settings["server"], settings["port"], settings["database"], settings["user"],
    )
    return pymssql.connect(**settings)


def load_bronze_to_mssql(bronze_dir=None, config_path="ingestion/config.yaml"):
    config = {}
    if os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}

    local_dir = bronze_dir or config.get("storage", {}).get("local_bronze_dir", "data/bronze")
    raw_schema = config.get("mssql", {}).get("raw_schema", "raw")

    ensure_database(config)
    conn = get_mssql_connection(config)
    cursor = conn.cursor()
    loaded_counts = {}

    try:
        cursor.execute(
            f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'{raw_schema}') "
            f"EXEC('CREATE SCHEMA {raw_schema}');"
        )

        for entity in ENTITIES:
            file_path = os.path.join(local_dir, f"{entity}.json")
            if not os.path.exists(file_path):
                logger.warning("Bronze file not found, skipping entity: %s", file_path)
                continue

            with open(file_path, "r", encoding="utf-8") as f:
                records = json.load(f)

            table_name = f"{raw_schema}.raw_{entity}"

            # Create-if-missing and TRUNCATE are separate statements so SQL Server
            # resolves the table name after it certainly exists.
            cursor.execute(
                f"""
                IF OBJECT_ID('{table_name}', 'U') IS NULL
                CREATE TABLE {table_name} (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    payload NVARCHAR(MAX) NOT NULL,
                    _ingested_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
                );
                """
            )
            cursor.execute(f"TRUNCATE TABLE {table_name};")

            insert_sql = f"INSERT INTO {table_name} (payload) VALUES (%s)"
            payloads = [(json.dumps(record),) for record in records]
            for start in range(0, len(payloads), INSERT_BATCH_SIZE):
                cursor.executemany(insert_sql, payloads[start:start + INSERT_BATCH_SIZE])

            loaded_counts[entity] = len(records)
            logger.info("Staged %s records for %s", len(records), table_name)

        # Single commit: either every raw table is refreshed, or none of them are.
        conn.commit()
        logger.info(
            "MSSQL raw load committed: %s",
            ", ".join(f"{k}={v}" for k, v in loaded_counts.items()) or "no entities loaded",
        )
        return loaded_counts
    except Exception:
        conn.rollback()
        logger.exception("MSSQL raw load failed and was rolled back; previous data is intact")
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    load_bronze_to_mssql()
