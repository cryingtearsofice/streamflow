# Power BI Dashboard Requirements

## Connecting Power BI to the Gold layer

The Gold DDL now exists for real (`sql/gold/create_gold_tables.sql`, merged via the Epic 11/12 PR) — `dim_date`, `dim_event_type`, `dim_account`, `fact_events`, `fact_transactions` are all defined, and there's a loader (`scripts/load_gold_to_snowflake.py`) plus a full orchestration DAG (`streamflow_snowflake_pipeline.py`). Before actually connecting, confirm someone has run that pipeline against a live Snowflake account — DDL existing isn't the same as the tables having real data. Steps once that's confirmed:

1. Open Power BI Desktop → **Get Data** → search for **Snowflake**.
2. Enter the server (`<account>.snowflakecomputing.com`) and warehouse from `config/snowflake.yml` (`STREAMFLOW_WH`).
3. Choose **Import** (not DirectQuery) for this project's scale, unless the team decides real-time refresh matters.
4. Sign in with a Snowflake account that has read access to the `ANALYTICS` schema — credentials come from whatever auth method is configured in `.env` (see `.env.example`), not typed directly into Power BI's saved connection.
5. Select the Gold tables: `dim_date`, `dim_event_type`, `dim_account`, `fact_events`, `fact_transactions`.
6. In the Model view, verify Power BI auto-detected the relationships between facts and dimensions; fix any that are missing or wrong (should join on the surrogate/natural keys defined in the Gold DDL).
7. Save as `powerbi/streamflow-dashboard.pbix`.

## Pages

### Overview page (KPI cards + trend/breakdown visuals — stories 11.2 & 11.3)

**KPI cards** (top row): `[Total Events]`, `[Distinct Accounts]`, `[Key Event Count]`, `[Event Rate]` — see [`powerbi/measures.md`](../powerbi/measures.md) for the DAX.

**Events Over Time** — line chart.
- X-axis: `dim_date[calendar_date]` (use the date hierarchy so it drills Year → Month → Day).
- Values: `[Total Events]`. Optionally add `[Key Event Count]` as a second line to visually compare total volume against successfully-posted transactions.

**Events by Source** — clustered column chart.
- Axis: `fact_events[source]`.
- Values: `[Total Events]`.

**Events by Event Type** — clustered column chart (or donut, if the team prefers proportion over absolute count).
- Axis: `dim_event_type[event_type]` (joined through `fact_events`).
- Values: `[Total Events]`.

**Filters/slicers** — placed at report level (not page level) so they also affect the KPI cards and the Transactions page (11.4):
- **Date** slicer bound to `dim_date[calendar_date]`, default range covering all available data.
- **Source** slicer bound to `fact_events[source]`.
- **Event Type** slicer bound to `dim_event_type[event_type]`.

### Transactions analysis page (story 11.4)

Same report-level Date/Source/Event Type filters from the Overview page apply here too.

**Transaction Amount Over Time** — line chart.
- X-axis: `dim_date[calendar_date]`.
- Values: `[Total Transaction Amount]`.
- Keep `[Average Transaction Amount]` as a *separate* visual below rather than a second line on the same chart — sum and average are on very different scales and mixing them on one axis is misleading.

**Average Transaction Amount Over Time** — line chart, same axis, `[Average Transaction Amount]` as the value. Placed directly under the total-amount chart so trends in volume vs. typical transaction size can be compared side by side.

**Status Breakdown** — donut or clustered column chart.
- Axis: `fact_transactions[status]` (`PENDING` / `POSTED` / `FAILED` / `REVERSED`).
- Values: `[Transaction Count]` — **not** `[Key Event Count]`, since that measure is pre-filtered to `POSTED` only and would make every other status show zero.

**Average Amount by Status** — clustered column chart.
- Axis: `fact_transactions[status]`.
- Values: `[Average Transaction Amount]`.
- Useful for spotting whether failed/reversed transactions skew toward unusually large or small amounts.

DAX measure definitions are in [`powerbi/measures.md`](../powerbi/measures.md), now checked against the real Gold DDL.

## Validation

Spot-checked against direct Snowflake queries on the Gold tables (2026-08-06), after a full clean pipeline run (500 raw -> 409 valid / 91 rejected -> Gold):

| KPI | Snowflake query result | Power BI card |
|---|---|---|
| Total Events | 409 | _confirm in PBI_ |
| Distinct Accounts | 50 | _confirm in PBI_ |
| Key Event Count (status = POSTED) | 97 | _confirm in PBI_ |
| Event Rate | 0.2372 | _confirm in PBI_ |
| Transaction Count | 409 | _confirm in PBI_ |
| Total Transaction Amount | $964,099.10 | _confirm in PBI_ |
| Average Transaction Amount | $2,357.21 | _confirm in PBI_ |

Queries used:
```sql
SELECT COUNT(*) FROM fact_events;
SELECT COUNT(DISTINCT account_key) FROM fact_events;
SELECT COUNT(*) FROM fact_transactions WHERE status = 'POSTED';
SELECT COUNT(*) FROM fact_transactions;
SELECT SUM(amount) FROM fact_transactions;
SELECT AVG(amount) FROM fact_transactions;
```
