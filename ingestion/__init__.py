"""
Ingestion package for the local ELT e-commerce warehouse.

Modules
-------
generate_data    Faker-based synthetic e-commerce event generator.
load_to_bronze   Lands generated records as JSON in the bronze zone (disk/MinIO).
load_to_mssql    Batch loads bronze JSON into the SQL Server `raw` schema.
load_to_postgres Batch loads bronze JSON into the PostgreSQL `raw` schema.
logging_config   Shared logging setup used by every module above.
"""

__all__ = [
    "generate_data",
    "load_to_bronze",
    "load_to_mssql",
    "load_to_postgres",
    "logging_config",
]
