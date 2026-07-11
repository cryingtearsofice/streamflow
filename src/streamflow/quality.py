from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DecimalType

from streamflow.schemas import (
	ALLOWED_EVENT_TYPES,
	ALLOWED_SOURCES,
	ALLOWED_STATUSES,
	AMOUNT_PATTERN,
	INVALID_REASONS,
	REQUIRED_FIELDS,
	TRANSACTION_SPARK_SCHEMA,
	UUID_PATTERN,
)


def _align_to_transaction_schema(df: DataFrame) -> DataFrame:
	aligned = df
	for field in TRANSACTION_SPARK_SCHEMA:
		if field.name not in aligned.columns:
			aligned = aligned.withColumn(field.name, F.lit(None).cast(field.dataType))
		else:
			aligned = aligned.withColumn(field.name, F.col(field.name).cast(field.dataType))

	ordered_columns = [field.name for field in TRANSACTION_SPARK_SCHEMA]
	return aligned.select(*ordered_columns)


def _is_missing(column_name: str) -> Column:
	col = F.col(column_name)
	return col.isNull() | (F.trim(col.cast("string")) == F.lit(""))

# Normalize incoming values.
def _clean_events(df: DataFrame) -> DataFrame:
	amount_as_string = F.trim(F.col("amount").cast("string"))
	stripped_amount = F.regexp_replace(
		F.regexp_replace(amount_as_string, r",", ""),
		r"^\$", "",
	)
	normalized_amount = (
		F.when(stripped_amount.rlike(r"^\d+$"), F.concat(stripped_amount, F.lit(".00")))
		.when(stripped_amount.rlike(r"^\d+\.\d$"), F.concat(stripped_amount, F.lit("0")))
		.otherwise(stripped_amount)
	)

	return (
		df.withColumn("event_id", F.trim(F.col("event_id").cast("string")))
		.withColumn("event_type", F.lower(F.trim(F.col("event_type").cast("string"))))
		.withColumn("source", F.lower(F.trim(F.col("source").cast("string"))))
		.withColumn("status", F.upper(F.trim(F.col("status").cast("string"))))
		.withColumn("account_id", F.trim(F.col("account_id").cast("string")))
		.withColumn("amount", normalized_amount)
	)

# Split incoming events into valid and rejected DataFrames with reason codes.
def apply_quality_rules(df: DataFrame) -> tuple[DataFrame, DataFrame]:
	transaction_df = _align_to_transaction_schema(df)
	cleaned_df = _clean_events(transaction_df)

	missing_conditions = [_is_missing(name) for name in REQUIRED_FIELDS]
	missing_any = missing_conditions[0]
	for condition in missing_conditions[1:]:
		missing_any = missing_any | condition

	missing_field_name = F.when(missing_conditions[0], F.lit(REQUIRED_FIELDS[0]))
	for i in range(1, len(REQUIRED_FIELDS)):
		missing_field_name = missing_field_name.when(
			missing_conditions[i], F.lit(REQUIRED_FIELDS[i])
		)

	invalid_event_type = ~F.col("event_type").isin(*ALLOWED_EVENT_TYPES)
	invalid_source = ~F.col("source").isin(*ALLOWED_SOURCES)
	invalid_status = ~F.col("status").isin(*ALLOWED_STATUSES)
	invalid_event_id = ~F.col("event_id").cast("string").rlike(UUID_PATTERN)
	invalid_event_ts = F.expr("try_cast(cast(event_ts as string) as timestamp)").isNull()
	invalid_amount = ~F.col("amount").cast("string").rlike(AMOUNT_PATTERN)

	duplicate_window = Window.partitionBy("event_id").orderBy(
		F.col("event_ts").asc_nulls_last()
	)
	duplicate_rank = F.row_number().over(duplicate_window)

	enriched = cleaned_df.withColumn("_duplicate_rank", duplicate_rank)

	reason_code = (
		F.when(missing_any, F.lit(INVALID_REASONS[0]))# MISSING_REQUIRED_FIELD
		.when(invalid_event_id, F.lit(INVALID_REASONS[1])) # INVALID_EVENT_ID
		.when(invalid_event_ts, F.lit(INVALID_REASONS[2])) # INVALID_EVENT_TS
		.when(invalid_amount, F.lit(INVALID_REASONS[3])) # INVALID_AMOUNT
		.when(invalid_event_type, F.lit(INVALID_REASONS[4])) # INVALID_EVENT_TYPE
		.when(invalid_source, F.lit(INVALID_REASONS[5])) # INVALID_SOURCE
		.when(invalid_status, F.lit(INVALID_REASONS[6])) # INVALID_STATUS
		.when(
			(~F.col("event_id").isNull()) & (F.col("_duplicate_rank") > 1),
			F.lit(INVALID_REASONS[7]), # DUPLICATE_EVENT_ID
		)
	)

	reason_detail = (
		F.when(missing_any, F.concat(F.lit("missing field: "), missing_field_name))
		.when(invalid_event_id, F.concat(F.lit("invalid event_id: "), F.col("event_id")))
		.when(invalid_event_ts, F.concat(F.lit("invalid event_ts: "), F.col("event_ts")))
		.when(invalid_amount, F.concat(F.lit("invalid amount: "), F.col("amount")))
		.when(invalid_event_type, F.concat(F.lit("invalid event_type: "), F.col("event_type")))
		.when(invalid_source, F.concat(F.lit("invalid source: "), F.col("source")))
		.when(invalid_status, F.concat(F.lit("invalid status: "), F.col("status")))
		.when(
			(~F.col("event_id").isNull()) & (F.col("_duplicate_rank") > 1),
			F.concat(F.lit("duplicate event_id: "), F.col("event_id")),
		)
	)

	with_reasons = (
		enriched.withColumn("reason_code", reason_code)
		.withColumn("reason_detail", reason_detail)
		.drop("_duplicate_rank")
	)

	valid_df = with_reasons.filter(F.col("reason_code").isNull()).drop("reason_code", "reason_detail").withColumn(
        "amount",
        F.col("amount").cast(DecimalType(10, 2))
    )
	rejected_df = with_reasons.filter(F.col("reason_code").isNotNull())

	return valid_df, rejected_df

# Creates the output paths of accepted and rejected records
def write_quality_outputs(
	valid_df: DataFrame,
	rejected_df: DataFrame,
	valid_path: str,
	reject_path: str,
	mode: str = "append",
	file_format: str = "parquet",
):
	valid_df.write.mode(mode).format(file_format).save(valid_path)
	rejected_df.write.mode(mode).format(file_format).save(reject_path)
