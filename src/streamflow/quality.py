from __future__ import annotations

from typing import Iterable

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window

from streamflow.schemas import TransactionSource, TransactionStatus, TransactionType


REQUIRED_FIELDS = (
	"event_id",
	"event_type",
	"event_ts",
	"source",
	"account_id",
	"amount",
	"status",
)

REASON_MISSING_REQUIRED_FIELD = "MISSING_REQUIRED_FIELD"
REASON_INVALID_EVENT_ID = "INVALID_EVENT_ID"
REASON_INVALID_EVENT_TS = "INVALID_EVENT_TS"
REASON_INVALID_AMOUNT = "INVALID_AMOUNT"
REASON_INVALID_EVENT_TYPE = "INVALID_EVENT_TYPE"
REASON_INVALID_SOURCE = "INVALID_SOURCE"
REASON_INVALID_STATUS = "INVALID_STATUS"
REASON_DUPLICATE_EVENT_ID = "DUPLICATE_EVENT_ID"

UUID_PATTERN = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
AMOUNT_PATTERN = r"^\$?\d{1,3}(,\d{3})*(\.\d{2})?$"

# Meant to turn enums into lists of allowed values
def _allowed_values(values: Iterable) -> list[str]:
	return [str(value.value) for value in values]


def _is_missing(column_name: str) -> Column:
	col = F.col(column_name)
	return col.isNull() | (F.trim(col.cast("string")) == F.lit(""))


def apply_quality_rules(df: DataFrame) -> tuple[DataFrame, DataFrame]:
	"""Split incoming events into valid and rejected DataFrames with reason codes."""
	allowed_event_types = _allowed_values(TransactionType)
	allowed_sources = _allowed_values(TransactionSource)
	allowed_statuses = _allowed_values(TransactionStatus)

	missing_conditions = [_is_missing(name) for name in REQUIRED_FIELDS]
	missing_any = missing_conditions[0]
	for condition in missing_conditions[1:]:
		missing_any = missing_any | condition

	missing_field_name = F.when(missing_conditions[0], F.lit(REQUIRED_FIELDS[0]))
	for i in range(1, len(REQUIRED_FIELDS)):
		missing_field_name = missing_field_name.when(
			missing_conditions[i], F.lit(REQUIRED_FIELDS[i])
		)

	invalid_event_type = ~F.col("event_type").isin(allowed_event_types)
	invalid_source = ~F.col("source").isin(allowed_sources)
	invalid_status = ~F.col("status").isin(allowed_statuses)
	invalid_event_id = ~F.col("event_id").cast("string").rlike(UUID_PATTERN)
	invalid_event_ts = F.expr("try_cast(cast(event_ts as string) as timestamp)").isNull()
	invalid_amount = ~F.col("amount").cast("string").rlike(AMOUNT_PATTERN)

	duplicate_window = Window.partitionBy("event_id").orderBy(
		F.col("event_ts").asc_nulls_last()
	)
	duplicate_rank = F.row_number().over(duplicate_window)

	enriched = df.withColumn("_duplicate_rank", duplicate_rank)

	reason_code = (
		F.when(missing_any, F.lit(REASON_MISSING_REQUIRED_FIELD))
		.when(invalid_event_id, F.lit(REASON_INVALID_EVENT_ID))
		.when(invalid_event_ts, F.lit(REASON_INVALID_EVENT_TS))
		.when(invalid_amount, F.lit(REASON_INVALID_AMOUNT))
		.when(invalid_event_type, F.lit(REASON_INVALID_EVENT_TYPE))
		.when(invalid_source, F.lit(REASON_INVALID_SOURCE))
		.when(invalid_status, F.lit(REASON_INVALID_STATUS))
		.when(
			(~F.col("event_id").isNull()) & (F.col("_duplicate_rank") > 1),
			F.lit(REASON_DUPLICATE_EVENT_ID),
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

	valid_df = with_reasons.filter(F.col("reason_code").isNull())
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
