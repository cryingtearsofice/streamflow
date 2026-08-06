"""One-shot reset for local testing/demos.

Clears local streaming output + Spark checkpoints, and truncates the
corresponding Snowflake Bronze/Silver/Gold tables and clears their staged
files (Bronze/Silver COPY INTO uses FORCE=TRUE, so stale staged files get
reprocessed on every load unless the stage itself is cleared too).

Usage:
    docker compose -f docker/compose.yml stop spark producer
    python scripts/reset_pipeline.py
    docker compose -f docker/compose.yml up -d spark
    docker compose -f docker/compose.yml up producer

Requires SNOWFLAKE_USER / SNOWFLAKE_PASSWORD in the environment (source .env first).
"""
import os
import shutil
from pathlib import Path

import snowflake.connector
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent

LOCAL_DIRS_TO_CLEAR = [
    PROJECT_ROOT / "data" / "raw" / "events",
    PROJECT_ROOT / "data" / "valid" / "events",
    PROJECT_ROOT / "data" / "rejects" / "events",
]
CHECKPOINT_DIR = PROJECT_ROOT / "data" / "checkpoints" / "streaming_ingest"

# dim_date is intentionally excluded - it's a static calendar seed, not per-run data.
TABLES_TO_TRUNCATE = [
    "bronze_events_raw",
    "silver_events",
    "silver_rejected_events",
    "fact_events",
    "fact_transactions",
    "dim_account",
    "dim_event_type",
]


def reset_local_data():
    if CHECKPOINT_DIR.exists():
        shutil.rmtree(CHECKPOINT_DIR)
        print(f"removed {CHECKPOINT_DIR}")
    for d in LOCAL_DIRS_TO_CLEAR:
        if d.exists():
            shutil.rmtree(d)
        d.mkdir(parents=True, exist_ok=True)
        print(f"cleared {d}")


def reset_snowflake():
    config_path = PROJECT_ROOT / "config" / "snowflake.yml"
    with config_path.open() as f:
        config = yaml.safe_load(f)
    sf = config["snowflake"]
    pipeline = config["pipeline"]

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
        for table in TABLES_TO_TRUNCATE:
            cur.execute(f"TRUNCATE TABLE IF EXISTS {table}")
            print(f"truncated {table}")

        cur.execute(f"REMOVE @{pipeline['stage_name']}")
        print(f"cleared stage @{pipeline['stage_name']}")
        cur.execute(f"REMOVE @{pipeline['silver_stage_name']}/valid/events")
        print(f"cleared stage @{pipeline['silver_stage_name']}/valid/events")
        cur.execute(f"REMOVE @{pipeline['silver_stage_name']}/rejects/events")
        print(f"cleared stage @{pipeline['silver_stage_name']}/rejects/events")
    finally:
        conn.close()


def main():
    print("=== Clearing local data ===")
    reset_local_data()
    print("=== Resetting Snowflake (truncating tables + clearing stages) ===")
    reset_snowflake()
    print("=== Done. Bring spark back up, then run the producer, before re-triggering the pipeline. ===")


if __name__ == "__main__":
    main()
