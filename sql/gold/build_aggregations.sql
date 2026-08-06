USE ROLE __ROLE__;
USE WAREHOUSE __WAREHOUSE__;
USE DATABASE __DATABASE__;
USE SCHEMA __SCHEMA__;

MERGE INTO gold_daily_fluctuation_by_type AS t
USING (
    SELECT 
        t.date_key,
        d.calendar_date,
        -- Pivot financial volumes by transaction type name
        COALESCE(SUM(CASE WHEN LOWER(det.event_type) = 'deposit'    THEN t.amount ELSE 0 END), 0) AS rev_deposit,
        COALESCE(SUM(CASE WHEN LOWER(det.event_type) = 'withdrawal' THEN t.amount ELSE 0 END), 0) AS rev_withdrawal,
        COALESCE(SUM(CASE WHEN LOWER(det.event_type) = 'transfer'   THEN t.amount ELSE 0 END), 0) AS rev_transfer,
        COALESCE(SUM(CASE WHEN LOWER(det.event_type) = 'purchase'   THEN t.amount ELSE 0 END), 0) AS rev_purchase,
        COALESCE(SUM(CASE WHEN LOWER(det.event_type) = 'fee charge' THEN t.amount ELSE 0 END), 0) AS rev_fee_charge,
        COALESCE(SUM(CASE WHEN LOWER(det.event_type) = 'reversal'   THEN t.amount ELSE 0 END), 0) AS rev_reversal,
        -- Total successful revenue across all types combined
        COALESCE(SUM(t.amount), 0) AS total_combined_revenue
    FROM fact_transactions t
    JOIN dim_event_type det ON t.event_type_key = det.event_type_key
    JOIN dim_date d         ON t.date_key = d.date_key
    WHERE LOWER(t.status) = 'posted'
    GROUP BY t.date_key, d.calendar_date
) AS s
ON t.date_key = s.date_key
WHEN MATCHED THEN 
    UPDATE SET 
        target.rev_deposit            = source.rev_deposit,
        target.rev_withdrawal         = source.rev_withdrawal,
        target.rev_transfer           = source.rev_transfer,
        target.rev_purchase           = source.rev_purchase,
        target.rev_fee_charge         = source.rev_fee_charge,
        target.rev_reversal           = source.rev_reversal,
        target.total_combined_revenue = source.total_combined_revenue,
        target.updated_at             = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN 
    INSERT (
        date_key, 
        calendar_date, 
        rev_deposit, 
        rev_withdrawal, 
        rev_transfer, 
        rev_purchase, 
        rev_fee_charge, 
        rev_reversal, 
        total_combined_revenue
    )
    VALUES (
        source.date_key, 
        source.calendar_date, 
        source.rev_deposit, 
        source.rev_withdrawal, 
        source.rev_transfer, 
        source.rev_purchase, 
        source.rev_fee_charge, 
        source.rev_reversal, 
        source.total_combined_revenue
    );

TRUNCATE TABLE gold_weekday_transaction_averages;

INSERT INTO gold_weekday_averages (day_of_week, day_name, avg_daily_transactions)
WITH daily_transaction_counts AS (
    SELECT 
        d.day_of_week,
        d.day_name,
        d.calendar_date,
        COUNT(t.transaction_id) AS total_tx_on_this_date
    FROM fact_transactions t
    JOIN dim_date d ON t.date_key = d.date_key
    GROUP BY d.day_of_week, d.day_name, d.calendar_date
)
SELECT 
    day_of_week,
    day_name,
    AVG(total_tx_on_this_date) AS avg_daily_transactions
FROM daily_transaction_counts
GROUP BY day_of_week, day_name
ORDER BY day_of_week ASC;

MERGE INTO gold_source_popularity AS t
USING (
    SELECT 
        source,
        COUNT(transaction_id) AS total_transactions
    FROM fact_transactions
    GROUP BY source
    ORDER BY total_transactions DESC
) AS s
ON t.source = s.source
WHEN MATCHED THEN
    UPDATE SET
        t.total_transactions = s.total_transactions,
        t.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (source, total_transactions)
    VALUES (s.source, s.total_transactions);

TRUNCATE TABLE gold_success_percentages;

INSERT INTO gold_success_percentages (status, percentage_of_total)
SELECT 
    t.status,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage_of_total
FROM fact_transactions t
GROUP BY t.status
ORDER BY percentage_of_total DESC;

MERGE INTO gold_account_revenue AS t
USING (
    SELECT 
        a.account_key,
        a.account_id,
        COALESCE(SUM(
            CASE 
                WHEN LOWER(det.event_type) = 'deposit'    THEN t.amount
                WHEN LOWER(det.event_type) = 'reversal'   THEN t.amount
                
                WHEN LOWER(det.event_type) = 'withdrawal' THEN -t.amount
                WHEN LOWER(det.event_type) = 'fee charge' THEN -t.amount
                WHEN LOWER(det.event_type) = 'purchase'   THEN -t.amount
                
                WHEN LOWER(det.event_type) = 'transfer'   THEN 0.00
                
                ELSE 0.00 
            END
        ), 0) AS total_revenue
    FROM dim_account a
    LEFT JOIN fact_transactions t ON a.account_key = t.account_key AND LOWER(t.status) = 'posted'
    LEFT JOIN dim_event_type det  ON t.event_type_key = det.event_type_key
    GROUP BY a.account_key, a.account_id
    ORDER BY total_revenue DESC
) AS s
ON t.account_key = s.account_key
WHEN MATCHED THEN
    UPDATE SET
        t.total_revenue = s.total_revenue,
        t.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (account_key, account_id, total_revenue)
    VALUES (s.account_key, s.account_id, s.total_revenue);