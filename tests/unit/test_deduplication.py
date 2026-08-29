from pyspark.sql import SparkSession

from iot_platform.transforms.deduplication import deduplicate_events


def test_duplicate_events_are_removed():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-deduplication")
        .getOrCreate()
    )

    try:
        data = [
            ("event-001", "device-001", 20.5),
            ("event-001", "device-001", 20.5),
            ("event-002", "device-002", 21.2),
        ]

        df = spark.createDataFrame(
            data,
            [
                "event_id",
                "device_id",
                "temperature",
            ],
        )

        result = deduplicate_events(df)

        assert result.count() == 2

    finally:
        spark.stop()