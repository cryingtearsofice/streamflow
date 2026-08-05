USE ROLE __ROLE__;
USE WAREHOUSE __WAREHOUSE__;

CREATE DATABASE IF NOT EXISTS __DATABASE__;
CREATE SCHEMA IF NOT EXISTS __DATABASE__.__SCHEMA__;

USE DATABASE __DATABASE__;
USE SCHEMA __SCHEMA__;

CREATE SEQUENCE IF NOT EXISTS seq_dim_event_type START = 1 INCREMENT = 1;
CREATE SEQUENCE IF NOT EXISTS seq_dim_account START = 1 INCREMENT = 1;

/* Create date dimension table. */
CREATE TABLE IF NOT EXISTS dim_date (
    date_key            INT PRIMARY KEY,
    calendar_date       DATE NOT NULL,
    day_of_week         INT,
    day_name            STRING,
    day_of_month        INT,
    month_number        INT,
    month_name          STRING,
    quarter_period      INT,
    numeric_year        INT,
    is_weekend          BOOLEAN
);

/* Create event type dimension table. */
CREATE TABLE IF NOT EXISTS dim_event_type (
    event_type_key      INT DEFAULT seq_dim_event_type.NEXTVAL PRIMARY KEY,
    event_type          STRING UNIQUE,
    loaded_at           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

/* Create account dimension table. */
CREATE TABLE IF NOT EXISTS dim_account (
    account_key         INT DEFAULT seq_dim_account.NEXTVAL PRIMARY KEY,
    account_id          STRING UNIQUE,
    first_seen_at       TIMESTAMP_NTZ,
    loaded_at           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

/* Create event facts table. */
CREATE TABLE IF NOT EXISTS fact_events (
    event_id            STRING PRIMARY KEY,
    date_key            INT REFERENCES dim_date(date_key),
    event_type_key      INT REFERENCES dim_event_type(event_type_key),
    account_key         INT REFERENCES dim_account(account_key),
    event_ts            TIMESTAMP_NTZ,
    source              STRING,
    kafka_partition     INTEGER,
    kafka_offset        INTEGER,
    loaded_at           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

/* Create transaction facts table. */
CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_id      STRING PRIMARY KEY,
    date_key            INT REFERENCES dim_date(date_key),
    event_type_key      INT REFERENCES dim_event_type(event_type_key),
    account_key         INT REFERENCES dim_account(account_key),
    event_ts            TIMESTAMP_NTZ,
    amount              DECIMAL(10, 2),
    status              STRING,
    source              STRING,
    loaded_at           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
