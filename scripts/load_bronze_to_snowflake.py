import argparse
import os
from pathlib import Path

import snowflake.connector
import yaml


def load_config(config_path: Path) -> dict:
    with config_path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def normalize_schema_config(database: str, schema_value: str) -> str:
    parts = schema_value.split(".")
    if len(parts) == 1:
        return schema_value
    if len(parts) == 2 and parts[0].upper() == database.upper():
        return parts[1]
    raise ValueError(
        "snowflake.schema must be either '<schema>' or '<database>.<schema>'"
    )


def build_tokens(config: dict, ingest_run_id: str) -> dict[str, str]:
    sf = config["snowflake"]
    pipeline = config["pipeline"]
    normalized_schema = normalize_schema_config(sf["database"], sf["schema"])
    return {
        "__ROLE__": sf["role"],
        "__WAREHOUSE__": sf["warehouse"],
        "__DATABASE__": sf["database"],
        "__SCHEMA__": normalized_schema,
        "__FILE_FORMAT__": pipeline["file_format"],
        "__STAGE__": pipeline["stage_name"],
        "__BRONZE_TABLE__": pipeline["bronze_table"],
        "__INGEST_RUN_ID__": ingest_run_id,
    }


def render_sql(template_text: str, tokens: dict[str, str]) -> str:
    rendered = template_text
    for key, value in tokens.items():
        rendered = rendered.replace(key, value)
    return rendered


def read_sql_file(path: Path) -> str:
    with path.open("r", encoding="utf-8") as handle:
        return handle.read()


def run_sql_script(cursor, sql_text: str, label: str) -> None:
    statements = [stmt.strip() for stmt in sql_text.split(";") if stmt.strip()]
    for statement in statements:
        try:
            cursor.execute(statement)
        except Exception as exc:
            raise RuntimeError(f"Failed statement in {label}: {statement}") from exc
    print(f"Executed {len(statements)} statement(s) from {label}")


def upload_parquet_files(cursor, stage_name: str, raw_events_path: Path) -> int:
    parquet_files = sorted(raw_events_path.rglob("*.parquet"))
    if not parquet_files:
        print(f"No parquet files found under {raw_events_path}")
        return 0

    uploaded = 0
    for parquet_file in parquet_files:
        file_uri = parquet_file.resolve().as_uri().replace("'", "''")
        put_sql = (
            f"PUT '{file_uri}' @{stage_name} "
            "AUTO_COMPRESS=FALSE OVERWRITE=FALSE"
        )
        cursor.execute(put_sql)
        uploaded += 1

    print(f"Uploaded {uploaded} parquet file(s) to @{stage_name}")
    return uploaded


def print_copy_history(cursor, database: str, schema: str, table: str) -> None:
    history_sql = f"""
    SELECT
      FILE_NAME,
      STATUS,
      ROW_COUNT,
      LAST_LOAD_TIME
    FROM TABLE(
      INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => '{database}.{schema}.{table}',
        START_TIME => DATEADD('day', -7, CURRENT_TIMESTAMP())
      )
    )
    ORDER BY LAST_LOAD_TIME DESC
    LIMIT 20
    """
    cursor.execute(history_sql)
    rows = cursor.fetchall()
    print("Recent COPY history (up to 20 rows):")
    for row in rows:
        print(row)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create Snowflake Bronze objects, upload parquet files, and COPY into bronze table."
    )
    parser.add_argument(
        "--config",
        default="config/snowflake.yml",
        help="Path to snowflake YAML config.",
    )
    parser.add_argument(
        "--ingest-run-id",
        default=None,
        help="Optional ingest run id override.",
    )
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        help="Skip PUT upload to stage.",
    )
    parser.add_argument(
        "--skip-copy",
        action="store_true",
        help="Skip COPY INTO execution.",
    )
    args = parser.parse_args()

    config_path = Path(args.config)
    config = load_config(config_path)

    pipeline = config["pipeline"]
    ingest_run_id = args.ingest_run_id or pipeline["ingest_run_id"]
    normalized_schema = normalize_schema_config(
        config["snowflake"]["database"], config["snowflake"]["schema"]
    )

    create_sql_path = Path(pipeline["sql_create_path"])
    load_sql_path = Path(pipeline["sql_load_path"])
    raw_events_path = Path(pipeline["raw_events_path"])

    tokens = build_tokens(config, ingest_run_id)

    conn = snowflake.connector.connect(
        account=config["snowflake"]["account"],
        user=require_env("SNOWFLAKE_USER"),
        password=require_env("SNOWFLAKE_PASSWORD"),
        role=config["snowflake"]["role"],
        warehouse=config["snowflake"]["warehouse"],
        database=config["snowflake"]["database"],
        schema=normalized_schema,
    )

    try:
        cursor = conn.cursor()
        create_sql = render_sql(read_sql_file(create_sql_path), tokens)
        run_sql_script(cursor, create_sql, str(create_sql_path))

        if not args.skip_upload:
            upload_parquet_files(cursor, tokens["__STAGE__"], raw_events_path)

        if not args.skip_copy:
            load_sql = render_sql(read_sql_file(load_sql_path), tokens)
            run_sql_script(cursor, load_sql, str(load_sql_path))
            print_copy_history(
                cursor,
                config["snowflake"]["database"],
                normalized_schema,
                pipeline["bronze_table"],
            )

    finally:
        conn.close()


if __name__ == "__main__":
    main()
