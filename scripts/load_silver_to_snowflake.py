import argparse
import os
from pathlib import Path

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
        "__STAGE__": pipeline["silver_stage_name"],
        "__BRONZE_TABLE__": pipeline["bronze_table"],
        "__SILVER_TABLE__": pipeline["silver_table"],
        "__SILVER_REJECTED_TABLE__": pipeline["silver_rejected_table"],
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


def stage_target_path(stage_name: str, stage_prefix: str, local_root: Path, parquet_file: Path) -> str:
    relative_parent = parquet_file.parent.relative_to(local_root).as_posix()
    if relative_parent == ".":
        return f"@{stage_name}/{stage_prefix}"
    return f"@{stage_name}/{stage_prefix}/{relative_parent}"


def upload_parquet_files(cursor, stage_name: str, local_root: Path, stage_prefix: str) -> int:
    parquet_files = sorted(local_root.rglob("*.parquet"))
    if not parquet_files:
        print(f"No parquet files found under {local_root}")
        return 0

    uploaded = 0
    for parquet_file in parquet_files:
        file_uri = parquet_file.resolve().as_uri().replace("%3D", "=").replace("'", "''")
        target_path = stage_target_path(stage_name, stage_prefix, local_root, parquet_file)
        put_sql = (
            f"PUT '{file_uri}' {target_path} "
            "AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        )
        cursor.execute(put_sql)
        uploaded += 1

    print(f"Uploaded {uploaded} parquet file(s) to @{stage_name}/{stage_prefix}")
    return uploaded


def print_reconciliation(cursor, sql_text: str) -> None:
    cursor.execute(sql_text)
    rows = cursor.fetchall()
    print("Silver reconciliation summary:")
    for row in rows:
        print(row)
    if rows and rows[0][-1] != "PASS":
        raise RuntimeError(f"Silver reconciliation failed: {rows[0]}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create Snowflake Silver objects, upload parquet files, merge into silver tables, and print reconciliation results."
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
        "--skip-merge",
        action="store_true",
        help="Skip MERGE execution.",
    )
    parser.add_argument(
        "--skip-checks",
        action="store_true",
        help="Skip reconciliation checks.",
    )
    args = parser.parse_args()

    config_path = Path(args.config)
    config = load_config(config_path)

    pipeline = config["pipeline"]
    ingest_run_id = args.ingest_run_id or pipeline["ingest_run_id"]
    normalized_schema = normalize_schema_config(
        config["snowflake"]["database"], config["snowflake"]["schema"]
    )

    create_sql_path = Path(pipeline["silver_sql_create_path"])
    load_sql_path = Path(pipeline["silver_sql_load_path"])
    checks_sql_path = Path(pipeline["silver_sql_quality_path"])
    valid_events_path = Path(pipeline["valid_events_path"])
    reject_events_path = Path(pipeline["reject_events_path"])

    tokens = build_tokens(config, ingest_run_id)

    import snowflake.connector

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
            upload_parquet_files(cursor, tokens["__STAGE__"], valid_events_path, "valid/events")
            upload_parquet_files(cursor, tokens["__STAGE__"], reject_events_path, "rejects/events")

        if not args.skip_merge:
            load_sql = render_sql(read_sql_file(load_sql_path), tokens)
            run_sql_script(cursor, load_sql, str(load_sql_path))

        if not args.skip_checks:
            checks_sql = render_sql(read_sql_file(checks_sql_path), tokens)
            print_reconciliation(cursor, checks_sql)

    finally:
        conn.close()


if __name__ == "__main__":
    main()