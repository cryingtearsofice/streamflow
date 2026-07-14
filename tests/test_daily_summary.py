from decimal import Decimal
from collections.abc import Iterator
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from spark.jobs.daily_summary import create_transaction_summary
from spark.jobs.daily_summary import write_summary


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
            "source": "mobile_app",
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
        ("withdrawal", "mobile_app"): {
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
            "source": "branch",
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
            "source": "branch",
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

    persisted_df = spark.read.parquet(output_path)

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