# Power BI Dashboard Requirements

## Connecting Power BI to the Gold layer

This can't be done yet — the Gold layer (Epic 9) doesn't exist in Snowflake yet. These are the intended steps once `dim_date`, `dim_event_type`, `dim_account`, `fact_events`, and `fact_transactions` are built:

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
- X-axis: `dim_date[date]` (use the date hierarchy so it drills Year → Month → Day).
- Values: `[Total Events]`. Optionally add `[Key Event Count]` as a second line to visually compare total volume against successfully-posted transactions.

**Events by Source** — clustered column chart.
- Axis: `fact_events[source]`.
- Values: `[Total Events]`.

**Events by Event Type** — clustered column chart (or donut, if the team prefers proportion over absolute count).
- Axis: `dim_event_type[event_type]` (joined through `fact_events`).
- Values: `[Total Events]`.

**Filters/slicers** — placed at report level (not page level) so they also affect the KPI cards and the Transactions page (11.4):
- **Date** slicer bound to `dim_date[date]`, default range covering all available data.
- **Source** slicer bound to `fact_events[source]`.
- **Event Type** slicer bound to `dim_event_type[event_type]`.

### Transactions analysis page (story 11.4)

Same report-level Date/Source/Event Type filters from the Overview page apply here too.

**Transaction Amount Over Time** — line chart.
- X-axis: `dim_date[date]`.
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

DAX measure definitions are drafted in [`powerbi/measures.md`](../powerbi/measures.md), ready to paste in once the Gold layer exists — column names there should be double-checked against whatever the finalized Gold DDL actually uses.

## Validation

Before treating any dashboard number as trustworthy, spot-check it against a direct Snowflake query on the Gold tables (per story 11.5) — document a couple of these comparisons here once done.
