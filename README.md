# streamflow

**Phase 1** — a synthetic banking transaction pipeline: a Python producer generates and validates events, publishes them to Kafka, Spark Structured Streaming ingests them into Parquet, and Airflow triggers a daily summary job.

**Phase 2** — an analytics layer on top of Phase 1: the same events flow through a Bronze/Silver/Gold medallion architecture in Snowflake, orchestrated end-to-end by a single Airflow DAG, and get visualized in a Power BI dashboard.

## Prerequisites

- **Docker Desktop** (with the WSL2 backend) — this is the only thing required to run the Phase 1 platform. Everything below assumes it's installed and running.
- **Python 3.13** — needed if you want to run `producer.py` or any of the Snowflake loader scripts (`scripts/`) directly on your machine instead of through Docker. Install once: `pip install -r docker/producer-requirements.txt`.
- **A Snowflake account** (Phase 2 only) — with credentials in a local `.env` file (copy `.env.example` and fill in `SNOWFLAKE_USER`/`SNOWFLAKE_PASSWORD`; `.env` is gitignored, never commit real credentials).
- **Power BI Desktop** (Phase 2 dashboard only).

## Running the platform

From the project root:

```
docker compose -f docker/compose.yml up -d
```

This single command builds and starts everything:

| Service | What it does |
|---|---|
| `kafka` | Single-node Kafka broker (KRaft mode). Internal address `kafka:9092`. |
| `postgres` | Airflow's metadata database. |
| `airflow-init` | One-time setup: runs DB migrations and creates the admin user. Exits after it finishes — this is expected, not a failure. |
| `airflow-webserver` | Airflow UI at [localhost:8081](http://localhost:8081) — log in with `admin` / `admin`. (Mapped to host port 8081, not 8080, since 8080 is commonly already in use.) |
| `airflow-scheduler` | Runs and schedules the DAGs. |
| `producer` | Generates synthetic banking transaction events (some deliberately invalid, to exercise validation) and publishes them to the `streamflow.events` Kafka topic. Runs for a configured number of events, then exits — this is expected. |
| `spark` | Long-running Structured Streaming job. Consumes `streamflow.events` continuously and writes it out as Parquet. |

Check status with `docker compose -f docker/compose.yml ps`. `kafka`, `postgres`, and `airflow-webserver` should show `(healthy)`; `spark` and `airflow-scheduler` should show `Up`; `producer` and `airflow-init` are expected to show `Exited (0)` once they've done their job.

To stop everything: `docker compose -f docker/compose.yml down` (add `-v` to also wipe the Postgres volume, e.g. if Airflow's metadata gets into a bad state).

## Configuring the producer

Producer behavior is controlled by [config/producer_config.json](config/producer_config.json), not hardcoded:

- `event_interval_seconds` — delay between events.
- `total_events` — how many events to generate, or `null` to run continuously until stopped.
- `num_accounts` — size of the simulated account pool (events reuse these accounts rather than inventing a new one each time).
- `invalid_rate` — fraction of events (0–1) deliberately corrupted, to test downstream validation.
- `bootstrap_servers` / `topic` — Kafka connection details. When running via Docker Compose, `KAFKA_BOOTSTRAP_SERVERS=kafka:9092` is set as a container environment variable and overrides whatever's in the config file.

Edit the JSON file and re-run `docker compose -f docker/compose.yml up -d --build producer` to pick up changes.

## Triggering the daily summary DAG

1. Open [localhost:8081](http://localhost:8081), log in with `admin` / `admin`.
2. Find the `streamflow_daily_summary` DAG and un-pause it (or trigger it manually with the run button) — it also runs automatically once a day.
3. The DAG's single task runs `spark-submit` against `scripts/run_summary.py` inside the Airflow container, which reads `data/valid/events`, and writes the daily summary, status summary, and transaction details described below.

## Output locations

- `data/valid/events/` — Parquet output for quality-validated events from the Spark streaming ingestion job.
- `data/rejects/events/` — Parquet output for records that fail quality validation (includes reason codes).
- `data/raw/events/` — Bronze source parquet path used by Snowflake stage upload.
- `data/checkpoints/` — Spark's streaming checkpoint state (used to resume correctly after a restart; don't delete unless you want to reprocess from the start).
- `data/curated/daily_summary/` — event counts/amounts grouped by date, event type, and source.
- `data/curated/status_summary/` — event counts/amounts grouped by date, event type, and status.
- `data/curated/transaction_details/` — the individual valid transactions the summaries above are built from.

## Bronze Snowflake Load

1. Set credentials in your shell:
   - `export SNOWFLAKE_USER=<user>`
   - `export SNOWFLAKE_PASSWORD=<password>`
2. Update [config/snowflake.yml](config/snowflake.yml) with your Snowflake identifiers.
3. Run the loader from project root:
   `python scripts/load_bronze_to_snowflake.py`

What this does:

- Creates/ensures the Bronze file format, stage, and table.
- Uploads parquet files from `data/raw/events/` to the stage.
- Executes `COPY INTO bronze_events_raw`.
- Prints recent copy history for idempotency validation.

Useful flags:

- `--skip-upload` to run SQL only.
- `--skip-copy` to create objects/upload without loading.
- `--ingest-run-id <value>` to override the run id written to Bronze metadata.

## Silver Snowflake Load

1. Make sure the Spark ingestion job has produced `data/valid/events/` and `data/rejects/events/`.
2. Reuse the same Snowflake credentials as with Bronze:
   - `export SNOWFLAKE_USER=<user>`
   - `export SNOWFLAKE_PASSWORD=<password>`
3. Run the Silver loader from project root:
   `python scripts/load_silver_to_snowflake.py`

What this does:

- Creates/ensures the Silver file format, stage, and typed Silver tables.
- Uploads valid and rejected parquet files into separate stage prefixes.
- MERGEs valid records into `silver_events` on `event_id`.
- MERGEs rejected records into `silver_rejected_events` on Kafka partition/offset.
- Prints a Bronze-vs-Silver reconciliation summary for the active ingest run.

Useful flags:

- `--skip-upload` to merge from files already in the Silver stage.
- `--skip-merge` to create objects/upload without loading.
- `--skip-checks` to skip the reconciliation query.
- `--ingest-run-id <value>` to override the run id used for reconciliation and loaded metadata.

## Gold Snowflake Load

Run after Bronze and Silver have loaded successfully:
```
python scripts/load_gold_to_snowflake.py
```
Creates the Gold star schema (`dim_date`, `dim_event_type`, `dim_account`, `fact_events`, `fact_transactions`), upserts dimension keys from Silver, and loads the fact tables.

Useful flags: `--skip-ddl`, `--skip-dimensions`, `--skip-facts`.

## Full Snowflake pipeline (orchestrated end-to-end)

Rather than running the three loaders by hand, the `snowflake_pipeline` Airflow DAG runs the whole medallion pipeline in order: check source files → load Bronze → build Silver → quality checks → build Gold → publish a run summary.

One-time setup: this DAG reads Snowflake credentials from an Airflow **Connection** named `snowflake_default` (not from `.env` — Airflow tasks run in their own containers). Create it once:
```
docker compose -f docker/compose.yml exec airflow-scheduler airflow connections add snowflake_default --conn-type snowflake --conn-login "$env:SNOWFLAKE_USER" --conn-password "$env:SNOWFLAKE_PASSWORD"
```

Trigger it from the Airflow UI ([localhost:8081](http://localhost:8081)) or via CLI:
```
docker compose -f docker/compose.yml exec airflow-scheduler airflow dags trigger snowflake_pipeline
```

Every run appends a row to a `pipeline_run_summary` table in Snowflake (auto-created on first run, via `scripts/publish_run_summary.py`) recording row counts across Bronze/Silver/Gold and a `PASS`/`FAIL` reconciliation status. Query it in Snowsight:
```sql
SELECT * FROM pipeline_run_summary ORDER BY run_at DESC;
```

**Note:** Bronze/Silver's `COPY INTO` uses `FORCE=TRUE`, so re-running against the same staged files reprocesses them every time. Regenerating fresh demo data requires clearing the Snowflake stages too, not just the tables — see `scripts/reset_pipeline.py` below.

## Power BI dashboard

Once the Gold tables have data, connect Power BI Desktop to Snowflake (Get Data → Snowflake → Import mode) and follow [`docs/dashboard_requirements.md`](docs/dashboard_requirements.md) for the exact connection steps, page/visual layout, and DAX measures (also in [`powerbi/measures.md`](powerbi/measures.md)).

Power BI's Import mode is a snapshot, not live — after regenerating data, use **Home → Refresh** in Power BI Desktop to pull the latest numbers.

## Resetting for a clean run

`scripts/reset_pipeline.py` clears local streaming output + Spark checkpoints, truncates the Snowflake Bronze/Silver/Gold tables, and clears their staged files — use this before regenerating fresh demo data so old and new data don't mix:
```
docker compose -f docker/compose.yml stop spark producer
python scripts/reset_pipeline.py
docker compose -f docker/compose.yml up -d spark
docker compose -f docker/compose.yml up producer
```

## Running Kafka without Docker (not needed if using Compose)

The Compose setup above runs Kafka for you. This is only relevant if you want to run `producer.py` directly against a broker without Docker at all:

1. Download [Kafka 4.3.1](https://kafka.apache.org/community/downloads/).
2. Install its Python client: `pip install confluent-kafka`.
3. From the extracted Kafka folder, run in order:
   ```
   bin\windows\kafka-storage.bat random-uuid
   bin\windows\kafka-storage.bat format --standalone -t <uuid-from-previous-command> -c config\server.properties
   bin\windows\kafka-server-start.bat config\server.properties
   ```

## Daily Stand up Notes
[Google Doc Link](https://docs.google.com/document/d/1vBSWKS6I9iNOImxK-9I53IPVQxgbxC305xiBsHl-q8I/edit?usp=sharing)
