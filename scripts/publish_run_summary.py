"""Publish a per-run summary row to Snowflake, capturing Bronze/Silver/Gold counts.

Run as the final step of the snowflake_pipeline DAG, after Gold is built.
Creates pipeline_run_summary if it doesn't exist yet, and appends one row
per run - this is the audit trail for "what happened in this pipeline run."
"""
import argparse
import os
from datetime import datetime, timezone
from pathlib import Path

import snowflake.connector
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent

CREATE_SUMMARY_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS pipeline_run_summary (
    ingest_run_id STRING,
    run_at TIMESTAMP_NTZ,
    bronze_rows NUMBER,
    silver_valid_rows NUMBER,
    silver_rejected_rows NUMBER,
    gold_fact_events_rows NUMBER,
    gold_fact_transactions_rows NUMBER,
    reconciliation_status STRING
)
"""


def main():
    parser = argparse.ArgumentParser(description="Publish a run summary row to Snowflake.")
    parser.add_argument("--config", default="config/snowflake.yml")
    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = PROJECT_ROOT / config_path
    with config_path.open() as f:
        config = yaml.safe_load(f)
    sf = config["snowflake"]
    ingest_run_id = config["pipeline"]["ingest_run_id"]

    conn = snowflake.connector.connect(
        account=sf["account"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=sf["role"],
        warehouse=sf["warehouse"],
        database=sf["database"],
        schema=sf["schema"],
    )
    try:
        cur = conn.cursor()
        cur.execute(CREATE_SUMMARY_TABLE_SQL)

        cur.execute("SELECT COUNT(*) FROM bronze_events_raw WHERE ingest_run_id = %s", (ingest_run_id,))
        bronze_rows = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM silver_events WHERE ingest_run_id = %s", (ingest_run_id,))
        silver_valid_rows = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM silver_rejected_events WHERE ingest_run_id = %s", (ingest_run_id,))
        silver_rejected_rows = cur.fetchone()[0]

        # Gold's fact tables aren't scoped by ingest_run_id, so these are running totals,
        # not just-this-run counts.
        cur.execute("SELECT COUNT(*) FROM fact_events")
        gold_fact_events_rows = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM fact_transactions")
        gold_fact_transactions_rows = cur.fetchone()[0]

        reconciliation_status = "PASS" if bronze_rows >= (silver_valid_rows + silver_rejected_rows) else "FAIL"

        cur.execute(
            """
            INSERT INTO pipeline_run_summary
                (ingest_run_id, run_at, bronze_rows, silver_valid_rows, silver_rejected_rows,
                 gold_fact_events_rows, gold_fact_transactions_rows, reconciliation_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                ingest_run_id,
                datetime.now(timezone.utc),
                bronze_rows,
                silver_valid_rows,
                silver_rejected_rows,
                gold_fact_events_rows,
                gold_fact_transactions_rows,
                reconciliation_status,
            ),
        )

        print("=== Pipeline Run Summary ===")
        print(f"ingest_run_id:                    {ingest_run_id}")
        print(f"bronze_rows (this run):            {bronze_rows}")
        print(f"silver_valid_rows (this run):       {silver_valid_rows}")
        print(f"silver_rejected_rows (this run):    {silver_rejected_rows}")
        print(f"gold_fact_events_rows (total):      {gold_fact_events_rows}")
        print(f"gold_fact_transactions_rows (total):{gold_fact_transactions_rows}")
        print(f"reconciliation_status:              {reconciliation_status}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
