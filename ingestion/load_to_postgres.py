"""
Postgres Raw Loader
Reads raw JSON artifacts from the bronze landing zone and inserts them into
the Postgres 'raw' schema as JSONB payloads with ELT ingestion metadata.
"""

import os
import json
import psycopg2
from psycopg2.extras import execute_values
import yaml

def get_db_connection(config=None):
    # Check environment variables first (Docker / Airflow), then fallback to config
    host = os.getenv("POSTGRES_HOST")
    port = int(os.getenv("POSTGRES_PORT", 5432))
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    dbname = os.getenv("POSTGRES_DB")

    if not all([host, user, password, dbname]) and config:
        db_cfg = config.get("database", {})
        host = host or db_cfg.get("host", "localhost")
        port = port or db_cfg.get("port", 5432)
        user = user or db_cfg.get("user", "postgres")
        password = password or db_cfg.get("password", "postgres")
        dbname = dbname or db_cfg.get("dbname", "warehouse")

    # If running locally outside Docker and host is set to 'postgres', fallback to 'localhost'
    if host == "postgres" and not os.path.exists("/.dockerenv"):
        host = "localhost"

    return psycopg2.connect(
        host=host or "localhost",
        port=port or 5432,
        user=user or "postgres",
        password=password or "postgres",
        dbname=dbname or "warehouse"
    )

def load_bronze_to_postgres(bronze_dir="data/bronze", config_path="ingestion/config.yaml"):
    config = {}
    if os.path.exists(config_path):
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)

    local_dir = bronze_dir or config.get("storage", {}).get("local_bronze_dir", "data/bronze")
    conn = get_db_connection(config)
    cursor = conn.cursor()

    entities = ["customers", "products", "sessions", "orders", "order_items"]

    try:
        # Ensure raw schema exists
        cursor.execute("CREATE SCHEMA IF NOT EXISTS raw;")

        for entity in entities:
            file_path = os.path.join(local_dir, f"{entity}.json")
            if not os.path.exists(file_path):
                print(f"Warning: File not found: {file_path}, skipping.")
                continue

            with open(file_path, "r", encoding="utf-8") as f:
                records = json.load(f)

            table_name = f"raw.raw_{entity}"

            # Create table if not exists
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                payload JSONB NOT NULL,
                _ingested_at TIMESTAMP NOT NULL DEFAULT NOW()
            );
            TRUNCATE TABLE {table_name};
            """
            cursor.execute(create_table_sql)

            # Insert records in batch
            insert_query = f"INSERT INTO {table_name} (payload) VALUES %s"
            batch_data = [(json.dumps(r),) for r in records]
            execute_values(cursor, insert_query, batch_data, page_size=1000)

            print(f"Loaded {len(records)} records into {table_name}")

        conn.commit()
        print("Postgres raw loading completed successfully.")
    except Exception as e:
        conn.rollback()
        print(f"Error loading to Postgres: {e}")
        raise e
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    load_bronze_to_postgres()
