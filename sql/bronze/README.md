# Bronze Snowflake Loading

This folder contains SQL used by `scripts/load_bronze_to_snowflake.py`.

## Files

- `create_bronze_tables.sql`: creates database/schema context objects for Bronze loading:
  - Parquet file format
  - Internal stage
  - `bronze_events_raw` table with payload and ingest metadata
- `load_bronze_events.sql`: runs `COPY INTO` from stage to Bronze table.

## Idempotency Strategy

`COPY INTO` is configured with `FORCE = FALSE`. Snowflake tracks loaded files in copy history.
When the same staged files are loaded again to the same target table and stage path, those files are skipped.

You can inspect recent load history with:

```sql
SELECT *
FROM TABLE(
  INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'STREAMFLOW_DB.ANALYTICS.BRONZE_EVENTS_RAW',
    START_TIME => DATEADD('day', -7, CURRENT_TIMESTAMP())
  )
)
ORDER BY LAST_LOAD_TIME DESC;
```
