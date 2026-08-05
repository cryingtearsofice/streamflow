USE ROLE __ROLE__;
USE WAREHOUSE __WAREHOUSE__;

CREATE DATABASE IF NOT EXISTS __DATABASE__;
CREATE SCHEMA IF NOT EXISTS __DATABASE__.__SCHEMA__;

USE DATABASE __DATABASE__;
USE SCHEMA __SCHEMA__;

MERGE INTO fact_events t
USING (
    SELECT 
        s.event_id,
        REPLACE(TO_DATE(s.event_ts)::STRING, '-', '')::INT AS date_key,
        dt.event_type_key,
        da.account_key,
        s.event_ts,
        s.source,
        s.kafka_partition,
        s.kafka_offset
    FROM __SILVER_TABLE__ s
    LEFT JOIN dim_event_type dt ON s.event_type = dt.event_type
    LEFT JOIN dim_account da ON s.account_id = da.account_id
) s
ON t.event_id = s.event_id
WHEN NOT MATCHED THEN INSERT 
    (event_id, date_key, event_type_key, account_key, event_ts, source, kafka_partition, kafka_offset)
VALUES 
    (s.event_id, s.date_key, s.event_type_key, s.account_key, s.event_ts, s.source, s.kafka_partition, s.kafka_offset);


MERGE INTO fact_transactions t
USING (
    SELECT 
        s.event_id AS transaction_id,
        REPLACE(TO_DATE(s.event_ts)::STRING, '-', '')::INT AS date_key,
        dt.event_type_key,
        da.account_key,
        s.event_ts,
        s.amount,
        s.status,
        s.source
    FROM __SILVER_TABLE__ s
    LEFT JOIN dim_event_type dt ON s.event_type = dt.event_type
    LEFT JOIN dim_account da ON s.account_id = da.account_id
    WHERE s.amount IS NOT NULL
) s
ON t.transaction_id = s.transaction_id
WHEN NOT MATCHED THEN INSERT 
    (transaction_id, date_key, event_type_key, account_key, event_ts, amount, status, source)
VALUES 
    (s.transaction_id, s.date_key, s.event_type_key, s.account_key, s.event_ts, s.amount, s.status, s.source);