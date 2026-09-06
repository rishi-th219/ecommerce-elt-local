"""
MSSQL Raw Loader
Reads raw JSON artifacts from bronze storage and inserts them into
Microsoft SQL Server 'raw' schema tables with NVARCHAR(MAX) JSON payloads.
"""

import os
import json
import yaml
import pymssql

def get_mssql_connection(config=None):
    server = os.getenv("MSSQL_SERVER", "localhost")
    port = int(os.getenv("MSSQL_PORT", 1433))
    user = os.getenv("MSSQL_USER", "sa")
    password = os.getenv("MSSQL_PASSWORD", "P@ssword#@219#")
    database = os.getenv("MSSQL_DB", "warehouse")

    if config and "mssql" in config:
        cfg = config["mssql"]
        server = os.getenv("MSSQL_SERVER", cfg.get("server", server))
        port = int(os.getenv("MSSQL_PORT", cfg.get("port", port)))
        user = os.getenv("MSSQL_USER", cfg.get("user", user))
        password = os.getenv("MSSQL_PASSWORD", cfg.get("password", password))
        database = os.getenv("MSSQL_DB", cfg.get("database", database))

    return pymssql.connect(
        server=server,
        port=port,
        user=user,
        password=password,
        database=database
    )

def load_bronze_to_mssql(bronze_dir="data/bronze", config_path="ingestion/config.yaml"):
    config = {}
    if os.path.exists(config_path):
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)

    local_dir = bronze_dir or config.get("storage", {}).get("local_bronze_dir", "data/bronze")
    conn = get_mssql_connection(config)
    cursor = conn.cursor()

    entities = ["customers", "products", "sessions", "orders", "order_items"]

    try:
        # Ensure raw schema exists
        cursor.execute("IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'raw') EXEC('CREATE SCHEMA raw');")
        conn.commit()

        for entity in entities:
            file_path = os.path.join(local_dir, f"{entity}.json")
            if not os.path.exists(file_path):
                print(f"Warning: File not found: {file_path}, skipping.")
                continue

            with open(file_path, "r", encoding="utf-8") as f:
                records = json.load(f)

            table_name = f"raw.raw_{entity}"

            create_table_sql = f"""
            IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name};
            CREATE TABLE {table_name} (
                id INT IDENTITY(1,1) PRIMARY KEY,
                payload NVARCHAR(MAX) NOT NULL,
                _ingested_at DATETIME2 NOT NULL DEFAULT GETUTCDATE()
            );
            """
            cursor.execute(create_table_sql)
            conn.commit()

            # Batch insert
            insert_sql = f"INSERT INTO {table_name} (payload) VALUES (%s)"
            batch_data = [(json.dumps(r),) for r in records]
            cursor.executemany(insert_sql, batch_data)
            conn.commit()

            print(f"Loaded {len(records)} records into MSSQL table {table_name}")

        print("MSSQL raw loading completed successfully.")
    except Exception as e:
        conn.rollback()
        print(f"Error loading to MSSQL: {e}")
        raise e
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    load_bronze_to_mssql()
