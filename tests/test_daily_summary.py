from decimal import Decimal
from collections.abc import Iterator
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from spark.jobs.daily_summary import (
    create_transaction_details,
    create_transaction_summary,
    write_summary,
    write_transaction_details,
)


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    session = (
        SparkSession.builder.master("local[2]")
        .appName("streamflow-daily-summary-tests")
        .getOrCreate()
    )
    yield session
    session.stop()


def _sample_valid_events(spark: SparkSession):
    return spark.createDataFrame(
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
                "event_type": "deposit",
                "source": "atm",
                "amount": Decimal("50.00"),
                "status": "PENDING",
            },
            {
                "event_id": "evt-3",
                "account_id": "acct-300",
                "event_ts": "2026-07-14T09:15:00Z",
                "event_type": "withdrawal",
                "source": "mobile app",
                "amount": Decimal("25.00"),
                "status": "POSTED",
            },
        ]
    )


def test_create_transaction_details_contains_one_row_per_valid_transaction(spark: SparkSession):
    valid_df = _sample_valid_events(spark)

    details_df = create_transaction_details(valid_df)

    assert details_df.count() == valid_df.count()
    assert details_df.columns == [
        "event_id",
        "account_id",
        "event_ts",
        "event_date",
        "event_type",
        "source",
        "amount",
        "status",
    ]



def test_transaction_summary_groups_by_event_date_event_type_source(spark: SparkSession):
    details_df = create_transaction_details(_sample_valid_events(spark))

    summary_df = create_transaction_summary(details_df)
    rows = {
        (str(row.event_date), row.event_type, row.source): {
            "event_count": row.event_count,
            "total_amount": Decimal(str(row.total_amount)),
            "avg_amount": Decimal(str(row.avg_amount)),
        }
        for row in summary_df.collect()
    }

    assert rows == {
        ("2026-07-13", "deposit", "atm"): {
            "event_count": 2,
            "total_amount": Decimal("150.00"),
            "avg_amount": Decimal("75.00"),
        },
        ("2026-07-14", "withdrawal", "mobile app"): {
            "event_count": 1,
            "total_amount": Decimal("25.00"),
            "avg_amount": Decimal("25.00"),
        },
    }


def test_transaction_summary_derives_event_date_from_event_ts_when_missing(spark: SparkSession):
    raw_df = _sample_valid_events(spark)

    summary_df = create_transaction_summary(raw_df)
    rows = {
        (str(row.event_date), row.event_type, row.source): row.event_count
        for row in summary_df.collect()
    }

    assert rows == {
        ("2026-07-13", "deposit", "atm"): 2,
        ("2026-07-14", "withdrawal", "mobile app"): 1,
    }



def test_status_summary_groups_by_event_date_event_type_status(spark: SparkSession):
    details_df = create_transaction_details(_sample_valid_events(spark))

    status_df = create_transaction_summary(
        details_df,
        group=["event_date", "event_type", "status"],
    )
    rows = {
        (str(row.event_date), row.event_type, row.status): {
            "event_count": row.event_count,
            "total_amount": Decimal(str(row.total_amount)),
            "avg_amount": Decimal(str(row.avg_amount)),
        }
        for row in status_df.collect()
    }

    assert rows == {
        ("2026-07-13", "deposit", "POSTED"): {
            "event_count": 1,
            "total_amount": Decimal("100.00"),
            "avg_amount": Decimal("100.00"),
        },
        ("2026-07-13", "deposit", "PENDING"): {
            "event_count": 1,
            "total_amount": Decimal("50.00"),
            "avg_amount": Decimal("50.00"),
        },
        ("2026-07-14", "withdrawal", "POSTED"): {
            "event_count": 1,
            "total_amount": Decimal("25.00"),
            "avg_amount": Decimal("25.00"),
        },
    }



def test_write_transaction_details_partitions_by_event_date(spark: SparkSession, tmp_path: Path):
    details_df = create_transaction_details(_sample_valid_events(spark))
    output_path = tmp_path / "transaction_details"

    write_transaction_details(details_df, str(output_path))

    partition_dirs = sorted(p.name for p in output_path.glob("event_date=*"))
    assert partition_dirs == ["event_date=2026-07-13", "event_date=2026-07-14"]



def test_write_summary_partitions_by_event_date(spark: SparkSession, tmp_path: Path):
    details_df = create_transaction_details(_sample_valid_events(spark))
    summary_df = create_transaction_summary(details_df)
    output_path = tmp_path / "daily_summary"

    write_summary(summary_df, str(output_path))

    partition_dirs = sorted(p.name for p in output_path.glob("event_date=*"))
    assert partition_dirs == ["event_date=2026-07-13", "event_date=2026-07-14"]



def test_write_status_summary_path_partitions_by_event_date(spark: SparkSession, tmp_path: Path):
    details_df = create_transaction_details(_sample_valid_events(spark))
    status_df = create_transaction_summary(
        details_df,
        group=["event_date", "event_type", "status"],
    )
    output_path = tmp_path / "status_summary"

    write_summary(status_df, str(output_path))

    partition_dirs = sorted(p.name for p in output_path.glob("event_date=*"))
    assert partition_dirs == ["event_date=2026-07-13", "event_date=2026-07-14"]


def test_write_summary_dynamic_overwrite_replaces_only_target_partition(
    spark: SparkSession, tmp_path: Path
):
    details_df = create_transaction_details(_sample_valid_events(spark))
    output_path = tmp_path / "daily_summary"

    initial_summary_df = create_transaction_summary(details_df)
    write_summary(initial_summary_df, str(output_path))

    updated_day_df = spark.createDataFrame(
        [
            {
                "event_id": "evt-4",
                "account_id": "acct-400",
                "event_ts": "2026-07-13T18:00:00Z",
                "event_type": "deposit",
                "source": "atm",
                "amount": Decimal("300.00"),
                "status": "POSTED",
            },
            {
                "event_id": "evt-5",
                "account_id": "acct-500",
                "event_ts": "2026-07-13T19:00:00Z",
                "event_type": "deposit",
                "source": "atm",
                "amount": Decimal("200.00"),
                "status": "POSTED",
            },
        ]
    )
    updated_details_df = create_transaction_details(updated_day_df)
    updated_summary_df = create_transaction_summary(updated_details_df)
    write_summary(updated_summary_df, str(output_path))

    persisted_df = spark.read.parquet(str(output_path))
    rows = {
        (str(row.event_date), row.event_type, row.source): {
            "event_count": row.event_count,
            "total_amount": Decimal(str(row.total_amount)),
            "avg_amount": Decimal(str(row.avg_amount)),
        }
        for row in persisted_df.collect()
    }

    assert rows == {
        ("2026-07-13", "deposit", "atm"): {
            "event_count": 2,
            "total_amount": Decimal("500.00"),
            "avg_amount": Decimal("250.00"),
        },
        ("2026-07-14", "withdrawal", "mobile app"): {
            "event_count": 1,
            "total_amount": Decimal("25.00"),
            "avg_amount": Decimal("25.00"),
        },
    }
