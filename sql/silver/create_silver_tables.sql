-- Tokens in this file are replaced by scripts/load_silver_to_snowflake.py.
-- Required tokens:
--   __ROLE__ __WAREHOUSE__ __DATABASE__ __SCHEMA__
--   __FILE_FORMAT__ __STAGE__ __SILVER_TABLE__ __SILVER_REJECTED_TABLE__

USE ROLE __ROLE__;
USE WAREHOUSE __WAREHOUSE__;

CREATE DATABASE IF NOT EXISTS __DATABASE__;
CREATE SCHEMA IF NOT EXISTS __DATABASE__.__SCHEMA__;

USE DATABASE __DATABASE__;
USE SCHEMA __SCHEMA__;

CREATE FILE FORMAT IF NOT EXISTS __FILE_FORMAT__
	TYPE = PARQUET
	COMPRESSION = AUTO;

CREATE STAGE IF NOT EXISTS __STAGE__
	FILE_FORMAT = __FILE_FORMAT__;

CREATE TABLE IF NOT EXISTS __SILVER_TABLE__ (
	schema_version STRING,
	event_id STRING NOT NULL,
	event_type STRING,
	event_ts TIMESTAMP_NTZ,
	source STRING,
	account_id STRING,
	amount DECIMAL(10, 2),
	status STRING,
	kafka_timestamp TIMESTAMP_NTZ,
	kafka_partition INTEGER,
	kafka_offset INTEGER,
	source_file STRING,
	ingest_run_id STRING,
	loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS __SILVER_REJECTED_TABLE__ (
	schema_version STRING,
	event_id STRING,
	event_type STRING,
	event_ts STRING,
	source STRING,
	account_id STRING,
	amount STRING,
	status STRING,
	kafka_timestamp TIMESTAMP_NTZ,
	kafka_partition INTEGER,
	kafka_offset INTEGER,
	reason_code STRING,
	reason_detail STRING,
	source_file STRING,
	ingest_run_id STRING,
	loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
