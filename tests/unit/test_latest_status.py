from datetime import datetime

from pyspark.sql import SparkSession

from iot_platform.transforms.gold import build_device_latest_status


def test_build_device_latest_status():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-latest-status")
        .getOrCreate()
    )

    try:
        data = [
            (
                "device-001",
                "industrial_cooler",
                datetime(2026, 9, 1, 10, 0),
                20.0,
                50.0,
                1000.0,
                90.0,
                -15.78,
                35.01,
                "v1.0.0",
                datetime(2026, 9, 1).date(),
                "event-001",
            ),
            (
                "device-001",
                "industrial_cooler",
                datetime(2026, 9, 1, 10, 5),
                25.0,
                55.0,
                1001.0,
                85.0,
                -15.78,
                35.01,
                "v1.0.0",
                datetime(2026, 9, 1).date(),
                "event-002",
            ),
            (
                "device-002",
                "pump",
                datetime(2026, 9, 1, 10, 2),
                30.0,
                60.0,
                1002.0,
                95.0,
                -15.79,
                35.02,
                "v2.0.0",
                datetime(2026, 9, 1).date(),
                "event-003",
            ),
        ]

        columns = [
            "device_id",
            "device_type",
            "event_timestamp",
            "temperature",
            "humidity",
            "pressure",
            "battery_level",
            "latitude",
            "longitude",
            "firmware_version",
            "event_date",
            "event_id",
        ]

        df = spark.createDataFrame(data, columns)

        result = build_device_latest_status(df)

        assert result.count() == 2

        device_001 = (
            result
            .filter("device_id = 'device-001'")
            .collect()[0]
        )

        assert device_001.event_timestamp == datetime(
            2026, 9, 1, 10, 5
        )

        assert device_001.temperature == 25.0
        assert device_001.humidity == 55.0
        assert device_001.battery_level == 85.0

    finally:
        spark.stop()