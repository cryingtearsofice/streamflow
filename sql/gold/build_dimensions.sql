USE ROLE __ROLE__;
USE WAREHOUSE __WAREHOUSE__;
USE DATABASE __DATABASE__;
USE SCHEMA __SCHEMA__;

/* Populate the calendar with a lengthy series of days, taking note of various aspects of each entry. */
INSERT INTO dim_date
SELECT 
    REPLACE(TO_DATE(date_val)::STRING, '-', '')::INT AS date_key,
    date_val AS calendar_date,
    DAYOFWEEK(date_val) AS day_of_week,
    DAYNAME(date_val) AS day_name,
    DAY(date_val) AS day_of_month,
    MONTH(date_val) AS month_number,
    MONTHNAME(date_val) AS month_name,
    QUARTER(date_val) AS quarter_period,
    YEAR(date_val) AS numeric_year,
    IFF(DAYOFWEEK(date_val) IN (6, 0), TRUE, FALSE) AS is_weekend -- If the day is Saturday (6) or Sunday (0), the weekend boolean will be set to true.
FROM (
    SELECT DATEADD(day, SEQ4(), '2026-01-01') AS date_val
    FROM TABLE(GENERATOR(ROWCOUNT => 3650)) -- Generate 10 years worth of dates, starting with Jan 1st, 2026. Allows for days with no records to still produce data when aggregated.
)
WHERE NOT EXISTS (SELECT 1 FROM dim_date LIMIT 1);

/* Select unique event types, insert them into the table. In the case of a missing type, insert it into the event type table. */
MERGE INTO dim_event_type t
USING (
    SELECT DISTINCT event_type 
    FROM __SILVER_TABLE__ 
    WHERE event_type IS NOT NULL
) s
ON t.event_type = s.event_type
WHEN NOT MATCHED THEN INSERT (event_type) VALUES (s.event_type); -- If the event type is unknown, insert.

/* Select unique account IDs, insert them into the table. If the account ID is now, insert into the table along with the first seen date. */
MERGE INTO dim_account t
USING (
    SELECT account_id, MIN(event_ts) AS first_seen 
    FROM __SILVER_TABLE__ 
    WHERE account_id IS NOT NULL 
    GROUP BY account_id
) s
ON t.account_id = s.account_id
WHEN NOT MATCHED THEN INSERT (account_id, first_seen_at) VALUES (s.account_id, s.first_seen); -- Create new account entry if previously unused.
