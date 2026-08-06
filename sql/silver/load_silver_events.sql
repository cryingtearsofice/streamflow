-- Tokens in this file are replaced by scripts/load_silver_to_snowflake.py.
-- Required tokens:
--   __ROLE__ __WAREHOUSE__ __DATABASE__ __SCHEMA__
--   __FILE_FORMAT__ __STAGE__ __SILVER_TABLE__ __SILVER_REJECTED_TABLE__
--   __INGEST_RUN_ID__

USE ROLE __ROLE__;
USE WAREHOUSE __WAREHOUSE__;
USE DATABASE __DATABASE__;
USE SCHEMA __SCHEMA__;

CREATE OR REPLACE TEMP TABLE __SILVER_TABLE___STAGE LIKE __SILVER_TABLE__;

COPY INTO __SILVER_TABLE___STAGE (
	schema_version,
	event_id,
	event_type,
	event_ts,
	source,
	account_id,
	amount,
	status,
	kafka_timestamp,
	kafka_partition,
	kafka_offset,
	source_file,
	ingest_run_id
)
FROM (
	SELECT
		$1:schema_version::STRING AS schema_version,
		$1:event_id::STRING AS event_id,
		$1:event_type::STRING AS event_type,
		TRY_TO_TIMESTAMP_NTZ($1:event_ts::STRING) AS event_ts,
		$1:source::STRING AS source,
		$1:account_id::STRING AS account_id,
		TRY_TO_DECIMAL($1:amount::STRING, 10, 2) AS amount,
		$1:status::STRING AS status,
		TRY_TO_TIMESTAMP_NTZ($1:kafka_timestamp::STRING) AS kafka_timestamp,
		$1:kafka_partition::INTEGER AS kafka_partition,
		$1:kafka_offset::INTEGER AS kafka_offset,
		METADATA$FILENAME AS source_file,
		'__INGEST_RUN_ID__' AS ingest_run_id
	FROM @__STAGE__/valid/events
)
FILE_FORMAT = (FORMAT_NAME = __FILE_FORMAT__)
ON_ERROR = 'CONTINUE'
FORCE = TRUE;

MERGE INTO __SILVER_TABLE__ AS target
USING (
	SELECT *
	FROM __SILVER_TABLE___STAGE
	QUALIFY ROW_NUMBER() OVER (
		PARTITION BY event_id
		ORDER BY source_file DESC, kafka_timestamp DESC, kafka_offset DESC
	) = 1
) AS source
ON target.event_id = source.event_id
WHEN MATCHED THEN UPDATE SET
	schema_version = source.schema_version,
	event_type = source.event_type,
	event_ts = source.event_ts,
	source = source.source,
	account_id = source.account_id,
	amount = source.amount,
	status = source.status,
	kafka_timestamp = source.kafka_timestamp,
	kafka_partition = source.kafka_partition,
	kafka_offset = source.kafka_offset,
	source_file = source.source_file,
	ingest_run_id = source.ingest_run_id
WHEN NOT MATCHED THEN INSERT (
	schema_version,
	event_id,
	event_type,
	event_ts,
	source,
	account_id,
	amount,
	status,
	kafka_timestamp,
	kafka_partition,
	kafka_offset,
	source_file,
	ingest_run_id
) VALUES (
	source.schema_version,
	source.event_id,
	source.event_type,
	source.event_ts,
	source.source,
	source.account_id,
	source.amount,
	source.status,
	source.kafka_timestamp,
	source.kafka_partition,
	source.kafka_offset,
	source.source_file,
	source.ingest_run_id
);

CREATE OR REPLACE TEMP TABLE __SILVER_REJECTED_TABLE___STAGE LIKE __SILVER_REJECTED_TABLE__;

COPY INTO __SILVER_REJECTED_TABLE___STAGE (
	schema_version,
	event_id,
	event_type,
	event_ts,
	source,
	account_id,
	amount,
	status,
	kafka_timestamp,
	kafka_partition,
	kafka_offset,
	reason_code,
	reason_detail,
	source_file,
	ingest_run_id
)
FROM (
	SELECT
		$1:schema_version::STRING AS schema_version,
		$1:event_id::STRING AS event_id,
		$1:event_type::STRING AS event_type,
		$1:event_ts::STRING AS event_ts,
		$1:source::STRING AS source,
		$1:account_id::STRING AS account_id,
		$1:amount::STRING AS amount,
		$1:status::STRING AS status,
		TRY_TO_TIMESTAMP_NTZ($1:kafka_timestamp::STRING) AS kafka_timestamp,
		$1:kafka_partition::INTEGER AS kafka_partition,
		$1:kafka_offset::INTEGER AS kafka_offset,
		$1:reason_code::STRING AS reason_code,
		$1:reason_detail::STRING AS reason_detail,
		METADATA$FILENAME AS source_file,
		'__INGEST_RUN_ID__' AS ingest_run_id
	FROM @__STAGE__/rejects/events
)
FILE_FORMAT = (FORMAT_NAME = __FILE_FORMAT__)
ON_ERROR = 'CONTINUE'
FORCE = TRUE;

MERGE INTO __SILVER_REJECTED_TABLE__ AS target
USING (
	SELECT *
	FROM __SILVER_REJECTED_TABLE___STAGE
	QUALIFY ROW_NUMBER() OVER (
		PARTITION BY kafka_partition, kafka_offset
		ORDER BY source_file DESC, kafka_timestamp DESC
	) = 1
) AS source
ON target.kafka_partition = source.kafka_partition
	AND target.kafka_offset = source.kafka_offset
WHEN MATCHED THEN UPDATE SET
	schema_version = source.schema_version,
	event_id = source.event_id,
	event_type = source.event_type,
	event_ts = source.event_ts,
	source = source.source,
	account_id = source.account_id,
	amount = source.amount,
	status = source.status,
	kafka_timestamp = source.kafka_timestamp,
	reason_code = source.reason_code,
	reason_detail = source.reason_detail,
	source_file = source.source_file,
	ingest_run_id = source.ingest_run_id
WHEN NOT MATCHED THEN INSERT (
	schema_version,
	event_id,
	event_type,
	event_ts,
	source,
	account_id,
	amount,
	status,
	kafka_timestamp,
	kafka_partition,
	kafka_offset,
	reason_code,
	reason_detail,
	source_file,
	ingest_run_id
) VALUES (
	source.schema_version,
	source.event_id,
	source.event_type,
	source.event_ts,
	source.source,
	source.account_id,
	source.amount,
	source.status,
	source.kafka_timestamp,
	source.kafka_partition,
	source.kafka_offset,
	source.reason_code,
	source.reason_detail,
	source.source_file,
	source.ingest_run_id
);

DROP TABLE IF EXISTS __SILVER_TABLE___STAGE;
DROP TABLE IF EXISTS __SILVER_REJECTED_TABLE___STAGE;
