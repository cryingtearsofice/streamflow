-- Tokens in this file are replaced by scripts/load_silver_to_snowflake.py.
-- Required tokens:
--   __DATABASE__ __SCHEMA__ __BRONZE_TABLE__
--   __SILVER_TABLE__ __SILVER_REJECTED_TABLE__ __INGEST_RUN_ID__

WITH bronze_counts AS (
	SELECT COUNT(*) AS bronze_rows
	FROM __DATABASE__.__SCHEMA__.__BRONZE_TABLE__
	WHERE ingest_run_id = '__INGEST_RUN_ID__'
),
silver_counts AS (
	SELECT COUNT(*) AS silver_rows
	FROM __DATABASE__.__SCHEMA__.__SILVER_TABLE__
	WHERE ingest_run_id = '__INGEST_RUN_ID__'
),
rejected_counts AS (
	SELECT COUNT(*) AS rejected_rows
	FROM __DATABASE__.__SCHEMA__.__SILVER_REJECTED_TABLE__
	WHERE ingest_run_id = '__INGEST_RUN_ID__'
)
SELECT
	bronze_counts.bronze_rows,
	silver_counts.silver_rows,
	rejected_counts.rejected_rows,
	(silver_counts.silver_rows + rejected_counts.rejected_rows) AS accounted_for_rows,
	(bronze_counts.bronze_rows - (silver_counts.silver_rows + rejected_counts.rejected_rows)) AS deduplicated_rows,
	CASE
		WHEN bronze_counts.bronze_rows >= (silver_counts.silver_rows + rejected_counts.rejected_rows)
		THEN 'PASS'
		ELSE 'FAIL'
	END AS reconciliation_status
FROM bronze_counts
CROSS JOIN silver_counts
CROSS JOIN rejected_counts;
