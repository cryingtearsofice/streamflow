# Power BI DAX Measures — KPI Summary Page

Column names below are confirmed against the real Gold DDL (`sql/gold/create_gold_tables.sql`) as of the Epic 11/12 merge — no longer guesses.

## Total Events

Count of all valid events that made it through Silver validation.

```dax
Total Events = COUNTROWS(fact_events)
```

## Distinct Accounts

Number of unique accounts represented in the event stream — the "distinct entities" KPI from the spec. `fact_events` doesn't carry `account_id` directly — it's a surrogate `account_key` joining to `dim_account`, but counting distinct keys is equivalent to counting distinct accounts.

```dax
Distinct Accounts = DISTINCTCOUNT(fact_events[account_key])
```

## Key Event Count (Posted Transactions)

The spec calls for a "key business event" count. For a banking domain, the most meaningful single event is a **successfully posted transaction** — i.e. `status = "POSTED"` in `fact_transactions`, as opposed to pending, failed, or reversed.

```dax
Key Event Count =
CALCULATE(
    COUNTROWS(fact_transactions),
    fact_transactions[status] = "POSTED"
)
```

## Event Rate (Posted Rate)

Fraction of all transactions that successfully posted — effectively a transaction success rate.

```dax
Event Rate = DIVIDE([Key Event Count], [Total Events])
```

## Total Transaction Amount (domain metric)

The payload-derived domain metric called for in the spec — total dollar volume moved.

```dax
Total Transaction Amount = SUM(fact_transactions[amount])
```

## Average Transaction Amount

Natural companion to the total — useful on the transactions analysis page (story 11.4).

```dax
Average Transaction Amount = AVERAGE(fact_transactions[amount])
```

## Transaction Count

Unlike `[Key Event Count]` (which is filtered to `POSTED` only), this counts transactions regardless of status — needed for the status-breakdown visual on the transactions analysis page (story 11.4), where the whole point is comparing counts *across* statuses.

```dax
Transaction Count = COUNTROWS(fact_transactions)
```

---

**Not separate measures** — these two KPIs from the spec are just groupings of `[Total Events]` on the visual, not new DAX:
- **Events by Source**: `[Total Events]` on a bar/column chart, axis = `fact_events[source]` (confirmed real column).
- **Events Over Time**: `[Total Events]` on a line chart, axis = `dim_date[calendar_date]` (not `[date]` — confirmed against the real DDL).

## Still worth double-checking before pasting into Power BI

- Whether the Snowflake pipeline (`scripts/load_gold_to_snowflake.py`, or the `streamflow_snowflake_pipeline` DAG) has actually been *run* against a live Snowflake account yet — DDL existing isn't the same as tables having real data in them.
- `fact_events`/`fact_transactions` both have a `-1` "UNASSIGNED" fallback row in `dim_event_type`/`dim_account` for unmatched foreign keys (see `create_gold_tables.sql`) — decide whether these fallback rows should be filtered out of KPI counts or intentionally included as a data-quality signal.
