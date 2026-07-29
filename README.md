# streamflow

A synthetic banking transaction pipeline: a Python producer generates and validates events, publishes them to Kafka, Spark Structured Streaming ingests them into Parquet, and Airflow triggers a daily summary job.

## Prerequisites

- **Docker Desktop** (with the WSL2 backend) — this is the only thing required to run the whole platform. Everything below assumes it's installed and running.
- **Python 3.13** — only needed if you want to run `producer.py` directly on your machine instead of through Docker (e.g. for quick iteration). If so, also install its dependencies: `pip install -r docker/producer-requirements.txt`.

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
| `airflow-webserver` | Airflow UI at [localhost:8080](http://localhost:8080) — log in with `admin` / `admin`. |
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

1. Open [localhost:8080](http://localhost:8080), log in with `admin` / `admin`.
2. Find the `streamflow_daily_summary` DAG and un-pause it (or trigger it manually with the run button) — it also runs automatically once a day.
3. The DAG's single task runs `spark-submit` against `spark/jobs/daily_summary.py` inside the Airflow container.

## Output locations

- `data/raw/events/` — Parquet output from the Spark streaming ingestion job.
- `data/checkpoints/` — Spark's streaming checkpoint state (used to resume correctly after a restart; don't delete unless you want to reprocess from the start).
- `data/rejects/`, `data/curated/` — reserved for data-quality and daily-summary outputs; not wired up yet.

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
