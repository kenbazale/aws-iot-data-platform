from pyspark.sql import SparkSession
from pathlib import Path

from iot_platform.transforms.incremental import (
    get_affected_partitions,partition_path,
)


def test_partition_path():
    result = partition_path(
        Path("data/silver"),
        "2026-08-29",
        18,
    )

    assert result == Path(
        "data/silver/"
        "event_date=2026-08-29/"
        "event_hour=18"
    )


def test_get_affected_partitions():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-incremental")
        .getOrCreate()
    )

    try:
        data = [
            ("2026-08-28", 23),
            ("2026-08-28", 23),
            ("2026-08-29", 18),
            ("2026-08-29", 18),
        ]

        df = spark.createDataFrame(
            data,
            ["event_date", "event_hour"],
        )

        df = df.withColumn(
            "event_date",
            df.event_date.cast("date"),
        )

        result = get_affected_partitions(df)

        assert set(result) == {
            ("2026-08-28", 23),
            ("2026-08-29", 18),
        }

    finally:
        spark.stop()