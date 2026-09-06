-- Initialize Airflow database
CREATE DATABASE airflow;

-- Switch to warehouse database to configure schemas
\c warehouse;

-- Raw schema: Landing zone for unnested/json ELT source tables
CREATE SCHEMA IF NOT EXISTS raw;

-- Staging schema: Type casting, column renaming, JSON extraction
CREATE SCHEMA IF NOT EXISTS staging;

-- Intermediate schema: Business logic, sessionization, profit margins
CREATE SCHEMA IF NOT EXISTS intermediate;

-- Marts schema: Gold Kimball star schema (dim_*, fct_*)
CREATE SCHEMA IF NOT EXISTS marts;

-- Ensure postgres user has complete privileges
GRANT ALL PRIVILEGES ON SCHEMA raw TO postgres;
GRANT ALL PRIVILEGES ON SCHEMA staging TO postgres;
GRANT ALL PRIVILEGES ON SCHEMA intermediate TO postgres;
GRANT ALL PRIVILEGES ON SCHEMA marts TO postgres;
