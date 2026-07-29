from decimal import Decimal
from collections.abc import Iterator
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from spark.jobs.daily_summary import create_transaction_summary
from spark.jobs.daily_summary import create_transaction_details
from spark.jobs.daily_summary import write_summary
from spark.jobs.daily_summary import write_transaction_details


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    session = (
        SparkSession.builder.master("local[2]")
        .appName("streamflow-daily-summary-tests")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_create_transaction_summary_groups_by_event_type_and_source(spark: SparkSession):
    events = [
        {
            "event_type": "deposit",
            "source": "atm",
            "amount": Decimal("100.00"),
        },
        {
            "event_type": "deposit",
            "source": "atm",
            "amount": Decimal("50.00"),
        },
        {
            "event_type": "withdrawal",
            "source": "atm",
            "amount": Decimal("25.00"),
        },
        {
            "event_type": "withdrawal",
            "source": "mobile app",
            "amount": Decimal("75.00"),
        },
    ]
    df = spark.createDataFrame(events)

    summary_df = create_transaction_summary(df)
    rows = {
        (row.event_type, row.source): {
            "event_count": row.event_count,
            "total_amount": Decimal(str(row.total_amount)),
            "avg_amount": Decimal(str(row.avg_amount)),
        }
        for row in summary_df.collect()
    }

    assert rows == {
        ("deposit", "atm"): {
            "event_count": 2,
            "total_amount": Decimal("150.00"),
            "avg_amount": Decimal("75.00"),
        },
        ("withdrawal", "atm"): {
            "event_count": 1,
            "total_amount": Decimal("25.00"),
            "avg_amount": Decimal("25.00"),
        },
        ("withdrawal", "mobile app"): {
            "event_count": 1,
            "total_amount": Decimal("75.00"),
            "avg_amount": Decimal("75.00"),
        },
    }


def test_create_transaction_summary_supports_custom_grouping(spark: SparkSession):
    events = [
        {
            "event_type": "deposit",
            "source": "atm",
            "amount": Decimal("100.00"),
        },
        {
            "event_type": "deposit",
            "source": "teller",
            "amount": Decimal("60.00"),
        },
        {
            "event_type": "withdrawal",
            "source": "atm",
            "amount": Decimal("30.00"),
        },
        {
            "event_type": "deposit",
            "source": "atm",
            "amount": Decimal("50.00"),
        },
    ]
    df = spark.createDataFrame(events)

    summary_df = create_transaction_summary(df, group="event_type")
    rows = {
        row.event_type: {
            "event_count": row.event_count,
            "total_amount": Decimal(str(row.total_amount)),
            "avg_amount": Decimal(str(row.avg_amount)),
        }
        for row in summary_df.collect()
    }

    assert rows == {
        "deposit": {
            "event_count": 3,
            "total_amount": Decimal("210.00"),
            "avg_amount": Decimal("70.00"),
        },
        "withdrawal": {
            "event_count": 1,
            "total_amount": Decimal("30.00"),
            "avg_amount": Decimal("30.00"),
        },
    }


def test_write_summary(spark: SparkSession, tmp_path: Path):
    events = [
        {
            "event_type": "deposit",
            "source": "atm",
            "amount": Decimal("100.00"),
        },
        {
            "event_type": "deposit",
            "source": "teller",
            "amount": Decimal("60.00"),
        },
        {
            "event_type": "withdrawal",
            "source": "atm",
            "amount": Decimal("30.00"),
        },
        {
            "event_type": "deposit",
            "source": "atm",
            "amount": Decimal("50.00"),
        },
    ]
    df = spark.createDataFrame(events)

    summary_df = create_transaction_summary(df, group="event_type")
    output_path = str(tmp_path / "daily_summary")

    write_summary(summary_df, output_path)

    write_summary(summary_df, output_path)

    persisted_df = spark.read.parquet(output_path)

    assert persisted_df.count() == 2

    rows = {
        row.event_type: {
            "event_count": row.event_count,
            "total_amount": Decimal(str(row.total_amount)),
            "avg_amount": Decimal(str(row.avg_amount)),
        }
        for row in persisted_df.collect()
    }

    assert rows == {
        "deposit": {
            "event_count": 3,
            "total_amount": Decimal("210.00"),
            "avg_amount": Decimal("70.00"),
        },
        "withdrawal": {
            "event_count": 1,
            "total_amount": Decimal("30.00"),
            "avg_amount": Decimal("30.00"),
        },
    }


def test_create_transaction_details_selects_correct_fields(spark: SparkSession):
    df = spark.createDataFrame(
        [
            {
                "event_id": "evt-1",
                "account_id": "acct-100",
                "event_ts": "2026-07-13T12:30:45Z",
                "event_type": "deposit",
                "source": "atm",
                "amount": Decimal("100.00"),
                "status": "POSTED",
                "extra_field": "ignore-me",
            }
        ]
    )

    details_df = create_transaction_details(df)
    row = details_df.collect()[0]

    assert details_df.columns == [
        "event_id",
        "account_id",
        "event_ts",
        "event_type",
        "source",
        "amount",
        "status",
    ]
    assert row.event_id == "evt-1"
    assert row.account_id == "acct-100"
    assert row.event_ts.isoformat() == "2026-07-13T12:30:45"
    assert row.event_type == "deposit"
    assert row.source == "atm"
    assert Decimal(str(row.amount)) == Decimal("100.00")
    assert row.status == "POSTED"


def test_create_transaction_details_preserves_multiple_transactions(spark: SparkSession):
    df = spark.createDataFrame(
        [
            {
                "event_id": "evt-1",
                "account_id": "acct-100",
                "event_ts": "2026-07-13T12:30:45Z",
                "event_type": "deposit",
                "source": "atm",
                "amount": Decimal("100.00"),
                "status": "POSTED",
                "ignored": "x",
            },
            {
                "event_id": "evt-2",
                "account_id": "acct-200",
                "event_ts": "2026-07-13T13:00:00Z",
                "event_type": "withdrawal",
                "source": "mobile app",
                "amount": Decimal("25.00"),
                "status": "PENDING",
                "ignored": "y",
            },
        ]
    )

    details_df = create_transaction_details(df)
    rows = {
        row.event_id: {
            "account_id": row.account_id,
            "event_ts": row.event_ts.isoformat(),
            "event_type": row.event_type,
            "source": row.source,
            "amount": Decimal(str(row.amount)),
            "status": row.status,
        }
        for row in details_df.collect()
    }

    assert details_df.count() == 2
    assert rows == {
        "evt-1": {
            "account_id": "acct-100",
            "event_ts": "2026-07-13T12:30:45",
            "event_type": "deposit",
            "source": "atm",
            "amount": Decimal("100.00"),
            "status": "POSTED",
        },
        "evt-2": {
            "account_id": "acct-200",
            "event_ts": "2026-07-13T13:00:00",
            "event_type": "withdrawal",
            "source": "mobile app",
            "amount": Decimal("25.00"),
            "status": "PENDING",
        },
    }


def test_write_transaction_details_overwrites_cleanly(spark: SparkSession, tmp_path: Path):
    first_df = spark.createDataFrame(
        [
            {
                "event_id": "evt-1",
                "account_id": "acct-100",
                "event_ts": "2026-07-13T12:30:45Z",
                "event_type": "deposit",
                "source": "atm",
                "amount": Decimal("100.00"),
                "status": "POSTED",
            },
            {
                "event_id": "evt-2",
                "account_id": "acct-200",
                "event_ts": "2026-07-13T13:00:00Z",
                "event_type": "withdrawal",
                "source": "mobile app",
                "amount": Decimal("25.00"),
                "status": "PENDING",
            },
        ]
    )

    details_df = create_transaction_details(first_df)
    output_path = str(tmp_path / "transaction_details")

    write_transaction_details(details_df, output_path)

    persisted_df = spark.read.parquet(output_path)
    rows = {
        row.event_id: {
            "account_id": row.account_id,
            "event_ts": row.event_ts.isoformat(),
            "event_type": row.event_type,
            "source": row.source,
            "amount": Decimal(str(row.amount)),
            "status": row.status,
        }
        for row in persisted_df.collect()
    }

    
    assert rows == {
        "evt-1": {
            "account_id": "acct-100",
            "event_ts": "2026-07-13T12:30:45",
            "event_type": "deposit",
            "source": "atm",
            "amount": Decimal("100.00"),
            "status": "POSTED",
        },
        "evt-2": {
            "account_id": "acct-200",
            "event_ts": "2026-07-13T13:00:00",
            "event_type": "withdrawal",
            "source": "mobile app",
            "amount": Decimal("25.00"),
            "status": "PENDING",
        },
    }
    second_df = spark.createDataFrame(
        [
            {
                "event_id": "evt-3",
                "account_id": "acct-100",
                "event_ts": "2026-07-13T12:30:45Z",
                "event_type": "deposit",
                "source": "atm",
                "amount": Decimal("100.00"),
                "status": "POSTED",
            },
            {
                "event_id": "evt-4",
                "account_id": "acct-200",
                "event_ts": "2026-07-13T13:00:00Z",
                "event_type": "withdrawal",
                "source": "mobile app",
                "amount": Decimal("25.00"),
                "status": "PENDING",
            },
        ]
    )

    details_df = create_transaction_details(second_df)

    write_transaction_details(details_df, output_path)

    persisted_df = spark.read.parquet(output_path)
    rows = {
        row.event_id: {
            "account_id": row.account_id,
            "event_ts": row.event_ts.isoformat(),
            "event_type": row.event_type,
            "source": row.source,
            "amount": Decimal(str(row.amount)),
            "status": row.status,
        }
        for row in persisted_df.collect()
    }

    
    assert rows == {
        "evt-3": {
            "account_id": "acct-100",
            "event_ts": "2026-07-13T12:30:45",
            "event_type": "deposit",
            "source": "atm",
            "amount": Decimal("100.00"),
            "status": "POSTED",
        },
        "evt-4": {
            "account_id": "acct-200",
            "event_ts": "2026-07-13T13:00:00",
            "event_type": "withdrawal",
            "source": "mobile app",
            "amount": Decimal("25.00"),
            "status": "PENDING",
        },
    }
    
    assert persisted_df.count() == 2