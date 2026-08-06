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

/* Create fallback rows to prevent null returns. */
INSERT INTO dim_event_type (event_type_key, event_type)
SELECT -1, 'UNASSIGNED_EVENT_TYPE'
WHERE NOT EXISTS (SELECT 1 FROM dim_event_type WHERE event_type_key = -1);

INSERT INTO dim_account (account_key, account_id, first_seen_at)
SELECT -1, 'UNASSIGNED_ACCOUNT', '1970-01-01 00:00:00'::TIMESTAMP_NTZ
WHERE NOT EXISTS (SELECT 1 FROM dim_account WHERE account_key = -1);

/* Create aggregation tables. */
/* Tallies the total daily change in revenue by transaction type. */
CREATE TABLE IF NOT EXISTS gold_daily_fluctuation_by_type (
    date_key                INT PRIMARY KEY REFERENCES dim_date(date_key),
    calendar_date           DATE NOT NULL,
    rev_deposit             DECIMAL(18, 2) DEFAULT 0.00,
    rev_withdrawal          DECIMAL(18, 2) DEFAULT 0.00,
    rev_transfer            DECIMAL(18, 2) DEFAULT 0.00,
    rev_purchase            DECIMAL(18, 2) DEFAULT 0.00,
    rev_fee_charge          DECIMAL(18, 2) DEFAULT 0.00,
    rev_reversal            DECIMAL(18, 2) DEFAULT 0.00,
    total_combined_revenue  DECIMAL(18, 2) DEFAULT 0.00,
    updated_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

/* Averages the transaction counts by day of the week. */
CREATE TABLE IF NOT EXISTS gold_weekday_averages (
    day_of_week             INT PRIMARY KEY,
    day_name                STRING NOT NULL,
    avg_daily_transactions  DECIMAL(12, 2) NOT NULL,
    updated_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

/* Sums the total use of each transaction source to determine popularity. */
CREATE TABLE IF NOT EXISTS gold_source_popularity (
    source                  STRING PRIMARY KEY,
    total_transactions      INT DEFAULT 0,
    updated_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

/* Calculates the percentage of each transaction status relative to the total. */
CREATE TABLE IF NOT EXISTS gold_success_percentages (
    status                  STRING PRIMARY KEY,
    percentage_of_total     DECIMAL(5, 2) DEFAULT 0.00,
    updated_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

/* Calculates the total revenue for each account. Deposits and reversals are added, while withdrawals and fees are subtracted. Transfers are treated as net-neutral, as there's no way to determine the direction within the data. */
CREATE TABLE IF NOT EXISTS gold_account_revenue (
    account_key             INT PRIMARY KEY REFERENCES dim_account(account_key),
    account_id              STRING NOT NULL,
    total_revenue           DECIMAL(18, 2) DEFAULT 0.00,
    updated_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
