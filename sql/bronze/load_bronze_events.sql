-- Tokens in this file are replaced by scripts/load_bronze_to_snowflake.py.
-- Required tokens:
--   __ROLE__ __WAREHOUSE__ __DATABASE__ __SCHEMA__
--   __FILE_FORMAT__ __STAGE__ __BRONZE_TABLE__ __INGEST_RUN_ID__

USE ROLE __ROLE__;
USE WAREHOUSE __WAREHOUSE__;
USE DATABASE __DATABASE__;
USE SCHEMA __SCHEMA__;

-- Idempotency strategy:
-- FORCE=FALSE (default) makes COPY INTO skip files Snowflake already recorded
-- as loaded for this target table + stage path in copy history.
COPY INTO __BRONZE_TABLE__
	(
		raw_payload,
		kafka_timestamp,
		kafka_partition,
		kafka_offset,
		source_file,
		ingest_run_id
	)
FROM (
	SELECT
		PARSE_JSON($1:raw_payload::STRING) AS raw_payload,
		TRY_TO_TIMESTAMP_NTZ($1:kafka_timestamp::STRING) AS kafka_timestamp,
		TRY_TO_NUMBER(TO_VARCHAR($1:kafka_partition))::INTEGER AS kafka_partition,
		TRY_TO_NUMBER(TO_VARCHAR($1:kafka_offset))::INTEGER AS kafka_offset,
		METADATA$FILENAME AS source_file,
		'__INGEST_RUN_ID__' AS ingest_run_id
	FROM @__STAGE__
)
FILE_FORMAT = (FORMAT_NAME = __FILE_FORMAT__)
ON_ERROR = 'CONTINUE'
FORCE = FALSE;
