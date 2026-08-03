from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def create_transaction_summary(df: DataFrame, group: str | list[str] | None = None) -> DataFrame:
    summary_input_df = df
    if "event_date" not in summary_input_df.columns:
        if "event_ts" not in summary_input_df.columns:
            raise ValueError("Input must include event_ts to build summaries")
        summary_input_df = summary_input_df.withColumn(
            "event_date", F.to_date(F.to_timestamp(F.col("event_ts")))
        )

    if not group:
        group_columns = ["event_date", "event_type", "source"]
    elif isinstance(group, str):
        group_columns = [group]
    else:
        group_columns = group

    missing_columns = set(group_columns) - set(summary_input_df.columns)
    if missing_columns:
            raise ValueError(f"Grouping columns not found: {sorted(missing_columns)}")
    
    return summary_input_df.groupBy(*group_columns).agg(
        F.count("*").alias("event_count"),
        F.sum("amount").alias("total_amount"),
        F.avg("amount").alias("avg_amount"),
    )


def write_summary(summary_df: DataFrame, output_path: str = "data/curated/daily_summary"):
    (
        summary_df.write
        .mode("overwrite")  # Should probably change this to append. Will have to see during integration
        .partitionBy("event_date")
        .parquet(output_path)
    )

def create_transaction_details(valid_df: DataFrame):
    return valid_df.withColumn("event_ts", F.to_timestamp(F.col("event_ts"))).withColumn(
        "event_date", F.to_date("event_ts")
    ).select(
        "event_id",
        "account_id",
        "event_ts",
        "event_date",
        "event_type",
        "source",
        "amount",
        "status",
    )

def write_transaction_details(details_df, output_path = "data/curated/transaction_details"):
    (
        details_df.write
        .mode("overwrite") # Should probably change this to append. Will have to see during integration
        .partitionBy("event_date")
        .parquet(output_path)
    )