from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def create_transaction_summary(df: DataFrame, group: str | list[str] | None = None) -> DataFrame:
    if group is None:
        group_columns = ["event_type", "source"]
    elif isinstance(group, str):
        group_columns = [group]
    else:
        group_columns = group

    return df.groupBy(*group_columns).agg(
        F.count("*").alias("event_count"),
        F.sum("amount").alias("total_amount"),
        F.avg("amount").alias("avg_amount"),
    )


def write_summary(summary_df: DataFrame, output_path: str = "data/curated/daily_summary"):
    (
        summary_df.write
        .mode("overwrite")
        .parquet(output_path)
    )

def create_transaction_details(valid_df: DataFrame):
    return valid_df.withColumn("event_ts", F.to_timestamp_ntz("event_ts")).select(
        "event_id",
        "account_id",
        "event_ts",
        "event_type",
        "source",
        "amount",
        "status",
    )

def write_transaction_details(details_df, output_path = "data/curated/transaction_details"):
    (
        details_df.write
        .mode("overwrite")
        .parquet(output_path)
    )