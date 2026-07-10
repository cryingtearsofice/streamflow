from pathlib import Path
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession

from streamflow.quality import apply_quality_rules, write_quality_outputs


UUID_1 = "11111111-1111-1111-1111-111111111111"
UUID_2 = "22222222-2222-2222-2222-222222222222"
UUID_3 = "33333333-3333-3333-3333-333333333333"
UUID_4 = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
UUID_DUPE = "dddddddd-4444-4444-4444-444444444444"
UUID_5 = "30303030-3030-3030-3030-303030303030"
UUID_6 = "31313131-3131-3131-3131-313131313131"


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[2]")
        .appName("streamflow-quality-tests")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_missing_required_fields_are_rejected(spark: SparkSession):
    df = spark.createDataFrame(
        [
            {
                "event_id": UUID_1,
                "event_type": "deposit",
                "event_ts": "2026-01-01T00:00:00Z",
                "source": "atm",
                "account_id": "acct-1",
                "amount": "10.00",
                "status": "POSTED",
            },
            {
                "event_id": UUID_2,
                "event_type": "deposit",
                "event_ts": "2026-01-01T00:00:00Z",
                "source": "atm",
                "account_id": None,
                "amount": "11.00",
                "status": "POSTED",
            },
            {
                "event_id": UUID_3,
                "event_type": "deposit",
                "event_ts": "2026-01-01T00:00:00Z",
                "source": "atm",
                "account_id": "",
                "amount": "12.00",
                "status": "POSTED",
            },
            {
                "event_id": UUID_4,
                "event_type": "deposit",
                "event_ts": "2026-01-01T00:00:00Z",
                "source": "atm",
                "account_id": "acct-4",
                "amount": None,
                "status": "POSTED",
            },
        ]
    )

    valid_df, rejected_df = apply_quality_rules(df)

    assert valid_df.count() == 1
    assert rejected_df.count() == 3

    reason_codes = {row.reason_code for row in rejected_df.select("reason_code").collect()}
    assert reason_codes == {"MISSING_REQUIRED_FIELD"}


def test_unknown_event_type_and_source_are_rejected(spark: SparkSession):
    df = spark.createDataFrame(
        [
            {
                "event_id": UUID_1,
                "event_type": "unknown_type",
                "event_ts": "2026-01-01T00:00:00Z",
                "source": "atm",
                "account_id": "acct-10",
                "amount": "10.00",
                "status": "POSTED",
            },
            {
                "event_id": UUID_2,
                "event_type": "deposit",
                "event_ts": "2026-01-01T00:00:00Z",
                "source": "unknown_source",
                "account_id": "acct-11",
                "amount": "11.00",
                "status": "POSTED",
            },
            {
                "event_id": UUID_3,
                "event_type": "deposit",
                "event_ts": "2026-01-01T00:00:00Z",
                "source": "atm",
                "account_id": "acct-12",
                "amount": "12.00",
                "status": "POSTED",
            },
        ]
    )

    valid_df, rejected_df = apply_quality_rules(df)

    assert valid_df.count() == 1
    assert rejected_df.count() == 2

    pairs = {
        (row.event_id, row.reason_code)
        for row in rejected_df.select("event_id", "reason_code").collect()
    }
    assert pairs == {
        (UUID_1, "INVALID_EVENT_TYPE"),
        (UUID_2, "INVALID_SOURCE"),
    }


def test_duplicate_event_id_is_flagged_and_rejected(spark: SparkSession):
    df = spark.createDataFrame(
        [
            {
                "event_id": UUID_DUPE,
                "event_type": "deposit",
                "event_ts": "2026-01-01T00:00:00Z",
                "source": "atm",
                "account_id": "acct-20",
                "amount": "10.00",
                "status": "POSTED",
            },
            {
                "event_id": UUID_DUPE,
                "event_type": "deposit",
                "event_ts": "2026-01-01T00:00:01Z",
                "source": "atm",
                "account_id": "acct-20",
                "amount": "10.00",
                "status": "POSTED",
            },
        ]
    )

    valid_df, rejected_df = apply_quality_rules(df)

    assert valid_df.count() == 1
    assert rejected_df.count() == 1

    rejected = rejected_df.select("reason_code").collect()[0]
    assert rejected.reason_code == "DUPLICATE_EVENT_ID"


def test_rejects_are_written_to_separate_path_with_reason_code(
    tmp_path: Path, spark: SparkSession
):
    df = spark.createDataFrame(
        [
            {
                "event_id": UUID_5,
                "event_type": "unknown_type",
                "event_ts": "2026-01-01T00:00:00Z",
                "source": "atm",
                "account_id": "acct-30",
                "amount": "10.00",
                "status": "POSTED",
            },
            {
                "event_id": UUID_6,
                "event_type": "deposit",
                "event_ts": "2026-01-01T00:00:00Z",
                "source": "atm",
                "account_id": "acct-31",
                "amount": "10.00",
                "status": "POSTED",
            },
        ]
    )

    valid_df, rejected_df = apply_quality_rules(df)

    valid_path = str(tmp_path / "raw")
    reject_path = str(tmp_path / "rejects")
    write_quality_outputs(valid_df, rejected_df, valid_path, reject_path)

    persisted_valid = spark.read.parquet(valid_path)
    persisted_rejected = spark.read.parquet(reject_path)

    assert persisted_valid.count() == 1
    assert persisted_rejected.count() == 1
    assert "reason_code" in persisted_rejected.columns


def test_invalid_schema_types_and_formats_are_rejected(spark: SparkSession):
    df = spark.createDataFrame(
        [
            {
                "event_id": "evt-1",
                "event_type": "deposit",
                "event_ts": "2026-01-01T00:00:00Z",
                "source": "atm",
                "account_id": "acct-1",
                "amount": "10.00",
                "status": "POSTED",
            },
            {
                "event_id": UUID_1,
                "event_type": "deposit",
                "event_ts": "not-a-timestamp",
                "source": "atm",
                "account_id": "acct-2",
                "amount": "10.00",
                "status": "POSTED",
            },
            {
                "event_id": UUID_2,
                "event_type": "deposit",
                "event_ts": "2026-01-01T00:00:00Z",
                "source": "atm",
                "account_id": "acct-3",
                "amount": "12.345",
                "status": "POSTED",
            },
            {
                "event_id": UUID_3,
                "event_type": "deposit",
                "event_ts": "2026-01-01T00:00:00Z",
                "source": "atm",
                "account_id": "acct-4",
                "amount": "10.00",
                "status": "COMPLETE",
            },
            {
                "event_id": UUID_4,
                "event_type": "deposit",
                "event_ts": "2026-01-01T00:00:00Z",
                "source": "atm",
                "account_id": "acct-5",
                "amount": "$1,000.00",
                "status": "POSTED",
            },
        ]
    )

    valid_df, rejected_df = apply_quality_rules(df)

    assert valid_df.count() == 1
    assert rejected_df.count() == 4

    rejected_pairs = {
        (row.event_id, row.reason_code)
        for row in rejected_df.select("event_id", "reason_code").collect()
    }

    assert rejected_pairs == {
        ("evt-1", "INVALID_EVENT_ID"),
        (UUID_1, "INVALID_EVENT_TS"),
        (UUID_2, "INVALID_AMOUNT"),
        (UUID_3, "INVALID_STATUS"),
    }


def test_cleaning_normalizes_fields_and_amounts(spark: SparkSession):
    df = spark.createDataFrame(
        [
            {
                "event_id": f"  {UUID_1}  ",
                "event_type": "  DEPOSIT ",
                "event_ts": "2026-01-01T00:00:00Z",
                "source": "  ATM ",
                "account_id": "  acct-1  ",
                "amount": "$1,000.00",
                "status": " posted ",
            }
        ]
    )

    valid_df, rejected_df = apply_quality_rules(df)

    assert rejected_df.count() == 0
    assert valid_df.count() == 1
    assert "reason_code" not in valid_df.columns
    assert "reason_detail" not in valid_df.columns

    row = valid_df.select(
        "event_id", "event_type", "source", "account_id", "amount", "status"
    ).collect()[0]
    assert row.event_id == UUID_1
    assert row.event_type == "deposit"
    assert row.source == "atm"
    assert row.account_id == "acct-1"
    assert row.amount == Decimal("1000.00")
    assert row.status == "POSTED"


def test_amount_normalizes_integer_and_single_decimal_inputs(spark: SparkSession):
    df = spark.createDataFrame(
        [
            {
                "event_id": UUID_1,
                "event_type": "deposit",
                "event_ts": "2026-01-01T00:00:00Z",
                "source": "atm",
                "account_id": "acct-1",
                "amount": "1234",
                "status": "POSTED",
            },
            {
                "event_id": UUID_2,
                "event_type": "deposit",
                "event_ts": "2026-01-01T00:00:00Z",
                "source": "atm",
                "account_id": "acct-2",
                "amount": "1234.5",
                "status": "POSTED",
            },
        ]
    )

    valid_df, rejected_df = apply_quality_rules(df)

    assert rejected_df.count() == 0
    assert valid_df.count() == 2

    amounts_by_event_id = {
        row.event_id: row.amount
        for row in valid_df.select("event_id", "amount").collect()
    }
    assert amounts_by_event_id == {
        UUID_1: Decimal("1234.00"),
        UUID_2: Decimal("1234.50"),
    }
