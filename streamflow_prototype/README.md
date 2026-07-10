# Streamflow Prototype

This folder is a small local prototype for the Streamflow phase 1 project. It uses the existing schema but it is intentionally not wired to Kafka, Spark, or Postgres for simplicity's sake.

## Structure

- `producer/producer.py` prints sample transaction events as JSONL to the console.
- `spark/streaming_job.py` reads those JSONL records from a local file and validates them.
- `sql/init_tables.sql` defines simple tables for valid and invalid records.

## Run the code:

1. Install dependencies:

   ```bash
   pip install pydantic
   ```

2. Generate sample events and save them to a local file:

   ```bash
   python streamflow_prototype/producer/producer.py > streamflow_prototype/sample_events.jsonl
   ```

3. Validate the records with the prototype streaming job:

   ```bash
   python streamflow_prototype/spark/streaming_job.py streamflow_prototype/sample_events.jsonl
   ```

4. If you want to create the demo tables in Postgres later, run:

   ```bash
   psql -f streamflow_prototype/sql/init_tables.sql
   ```