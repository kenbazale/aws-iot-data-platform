from datetime import date

from pyspark.sql import SparkSession
import pytest

from iot_platform.transforms.gold import (
    build_device_daily_metrics,
    get_affected_dates,
    write_gold_partitions,
)


def test_device_daily_metrics():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-gold")
        .getOrCreate()
    )

    try:
        data = [
            ("device-001", date(2026, 8, 29), 20.0, 50.0, 80.0),
            ("device-001", date(2026, 8, 29), 22.0, 60.0, 70.0),
            ("device-001", date(2026, 8, 29), 24.0, 55.0, 65.0),
            ("device-002", date(2026, 8, 29), 30.0, 40.0, 90.0),
        ]

        df = spark.createDataFrame(
            data,
            [
                "device_id",
                "event_date",
                "temperature",
                "humidity",
                "battery_level",
            ],
        )

        result = build_device_daily_metrics(df)

        assert result.count() == 2

        device_001 = (
            result
            .filter("device_id = 'device-001'")
            .collect()[0]
        )

        assert device_001.event_count == 3
        assert device_001.avg_temperature == 22.0
        assert device_001.min_temperature == 20.0
        assert device_001.max_temperature == 24.0
        assert device_001.avg_humidity == 55.0
        assert device_001.min_battery_level == 65.0
        assert device_001.avg_battery_level == pytest.approx(71.66666666666667)

    finally:
        spark.stop()


def test_get_affected_dates():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-affected-dates")
        .getOrCreate()
    )

    try:
        data = [
            ("device-001", date(2026, 9, 2)),
            ("device-002", date(2026, 9, 1)),
            ("device-003", date(2026, 9, 2)),
            ("device-004", date(2026, 8, 31)),
            ("device-005", None),
        ]

        df = spark.createDataFrame(
            data,
            ["device_id", "event_date"],
        )

        result = get_affected_dates(df)

        assert result == [
            date(2026, 8, 31),
            date(2026, 9, 1),
            date(2026, 9, 2),
        ]

    finally:
        spark.stop()

def test_device_daily_metrics_are_idempotent(tmp_path):
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-gold-idempotency")
        .getOrCreate()
    )

    try:
        data = [
            ("device-001", date(2026, 9, 1), 20.0, 50.0, 80.0),
            ("device-001", date(2026, 9, 1), 22.0, 60.0, 70.0),
            ("device-002", date(2026, 9, 1), 30.0, 40.0, 90.0),
        ]

        df = spark.createDataFrame(
            data,
            [
                "device_id",
                "event_date",
                "temperature",
                "humidity",
                "battery_level",
            ],
        )

        gold_path = str(tmp_path / "device_daily_metrics")

        result = build_device_daily_metrics(df)

        (
            result
            .write
            .mode("overwrite")
            .partitionBy("event_date")
            .parquet(gold_path)
        )

        # Materialize the first result before the files are replaced.
        first_run = (
            spark.read
            .parquet(gold_path)
            .select("device_id", "event_date", "event_count")
            .orderBy("device_id")
            .collect()
        )

        (
            result
            .write
            .mode("overwrite")
            .partitionBy("event_date")
            .parquet(gold_path)
        )

        # Read the newly written data.
        second_run = (
            spark.read
            .parquet(gold_path)
            .select("device_id", "event_date", "event_count")
            .orderBy("device_id")
            .collect()
        )

        assert len(first_run) == 2
        assert len(second_run) == 2
        assert second_run == first_run

    finally:
        spark.stop()

def test_write_gold_partitions_preserves_unaffected_partitions(tmp_path):
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-gold-partition-overwrite")
        .getOrCreate()
    )

    try:
        gold_path = str(tmp_path / "device_daily_metrics")

        initial_data = [
            ("device-001", date(2026, 9, 1), 20.0),
            ("device-002", date(2026, 9, 1), 25.0),
            ("device-001", date(2026, 9, 2), 30.0),
            ("device-002", date(2026, 9, 2), 35.0),
        ]

        initial_df = spark.createDataFrame(
            initial_data,
            [
                "device_id",
                "event_date",
                "avg_temperature",
            ],
        )

        (
            initial_df
            .write
            .mode("overwrite")
            .partitionBy("event_date")
            .parquet(gold_path)
        )

        updated_data = [
            ("device-001", date(2026, 9, 2), 99.0),
            ("device-002", date(2026, 9, 2), 100.0),
        ]

        updated_df = spark.createDataFrame(
            updated_data,
            [
                "device_id",
                "event_date",
                "avg_temperature",
            ],
        )

        write_gold_partitions(
            updated_df,
            gold_path,
        )

        result = (
            spark.read
            .parquet(gold_path)
            .select(
                "device_id",
                "event_date",
                "avg_temperature",
            )
            .orderBy("event_date", "device_id")
            .collect()
        )

        assert len(result) == 4

        # 2026-09-01 must remain unchanged.
        assert result[0].device_id == "device-001"
        assert result[0].avg_temperature == 20.0

        assert result[1].device_id == "device-002"
        assert result[1].avg_temperature == 25.0

        # 2026-09-02 must contain the updated values.
        assert result[2].device_id == "device-001"
        assert result[2].avg_temperature == 99.0

        assert result[3].device_id == "device-002"
        assert result[3].avg_temperature == 100.0

    finally:
        spark.stop()