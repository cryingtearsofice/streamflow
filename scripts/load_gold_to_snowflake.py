import argparse
import os
from pathlib import Path
import yaml
import snowflake.connector

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
    raise ValueError("snowflake.schema must be either '<schema>' or '<database>.<schema>'")

def build_tokens(config: dict) -> dict[str, str]:
    sf = config["snowflake"]
    pipeline = config["pipeline"]
    normalized_schema = normalize_schema_config(sf["database"], sf["schema"])
    return {
        "__ROLE__": sf["role"],
        "__WAREHOUSE__": sf["warehouse"],
        "__DATABASE__": sf["database"],
        "__SCHEMA__": normalized_schema,
        "__SILVER_TABLE__": pipeline["silver_table"],
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

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Initialize Gold dimensional configurations, build tables, execute dimension upserts, and load fact data rows."
    )
    parser.add_argument(
        "--config",
        default="config/snowflake.yml", # Will throw errors if the script is run standalone, in that case prepend ../ to both this and the gold yml file paths
        help="Path to snowflake YAML configuration file.",
    )
    parser.add_argument(
        "--skip-ddl",
        action="store_true",
        help="Skip table schema initialization checks.",
    )
    parser.add_argument(
        "--skip-dimensions",
        action="store_true",
        help="Skip populating and upserting dimensional keys.",
    )
    parser.add_argument(
        "--skip-facts",
        action="store_true",
        help="Skip fact pipeline transformation and loading loops.",
    )
    parser.add_argument(
        "--skip-aggregations",
        action="store_true",
        help="Skip aggregation calculations.",
    )
    args = parser.parse_args()

    config_path = Path(args.config)
    config = load_config(config_path)

    pipeline = config["pipeline"]
    normalized_schema = normalize_schema_config(
        config["snowflake"]["database"], config["snowflake"]["schema"]
    )

    create_sql_path = Path(pipeline["gold_sql_create_path"])
    dimensions_sql_path = Path(pipeline["gold_sql_dimensions_path"])
    facts_sql_path = Path(pipeline["gold_sql_facts_path"])
    aggregations_sql_path = Path(pipeline["gold_sql_aggregations_path"])

    tokens = build_tokens(config)

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

        if not args.skip_ddl:
            create_sql = render_sql(read_sql_file(create_sql_path), tokens)
            run_sql_script(cursor, create_sql, str(create_sql_path))

        if not args.skip_dimensions:
            dimensions_sql = render_sql(read_sql_file(dimensions_sql_path), tokens)
            run_sql_script(cursor, dimensions_sql, str(dimensions_sql_path))

        if not args.skip_facts:
            facts_sql = render_sql(read_sql_file(facts_sql_path), tokens)
            run_sql_script(cursor, facts_sql, str(facts_sql_path))

        if not args.skip_aggregations:
            aggregations_sql = render_sql(read_sql_file(aggregations_sql_path), tokens)
            run_sql_script(cursor, aggregations_sql, str(aggregations_sql_path))

    finally:
        conn.close()

if __name__ == "__main__":
    main()
