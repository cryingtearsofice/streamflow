# Power BI DAX Measures — KPI Summary Page

Drafted ahead of the Gold layer existing (Epic 9). References `fact_events`, `fact_transactions`, and `dim_account` as specified in the Phase 2 backlog — adjust names here if the actual Gold DDL ends up differing.

## Total Events

Count of all valid events that made it through Silver validation.

```dax
Total Events = COUNTROWS(fact_events)
```

## Distinct Accounts

Number of unique accounts represented in the event stream — the "distinct entities" KPI from the spec, using `account_id` as our entity.

```dax
Distinct Accounts = DISTINCTCOUNT(fact_events[account_id])
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
- **Events by Source**: `[Total Events]` on a bar/column chart, axis = `fact_events[source]` (or `dim_event_type` if source lives there).
- **Events Over Time**: `[Total Events]` on a line chart, axis = `dim_date[date]`.

## Open question for whoever finishes the Gold schema

Confirm the exact column names (`account_id` vs. a surrogate `account_key`, `status` vs. an enum-coded column) match what's actually in `fact_events`/`fact_transactions` before pasting these in — written against the names used in the backlog stories, not a finalized DDL.
