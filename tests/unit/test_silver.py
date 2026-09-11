from pyspark.sql import SparkSession

from iot_platform.transforms.silver import (
    transform_to_silver,
)


def test_silver_keeps_valid_unique_events():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-silver")
        .getOrCreate()
    )

    try:
        data = [
            (
                "event-001",
                " device-001 ",
                "Industrial_Cooler",
                "2026-08-29 10:00:00",
                20.5,
                50.0,
                1.8,
                99.0,
                -16.3,
                35.2,
                "1.0.0",
                None,
                True,
                "2026-08-29 10:05:00",
                "2026-08-29",
                10,
            ),
            (
                "event-001",
                " device-001 ",
                "Industrial_Cooler",
                "2026-08-29 10:00:00",
                20.5,
                50.0,
                1.8,
                99.0,
                -16.3,
                35.2,
                "1.0.0",
                None,
                True,
                "2026-08-29 10:05:00",
                "2026-08-29",
                10,
            ),
            (
                "event-002",
                "device-002",
                "Industrial_Cooler",
                "2026-08-29 10:01:00",
                21.2,
                55.0,
                1.7,
                98.0,
                -15.7,
                33.1,
                "1.0.0",
                None,
                True,
                "2026-08-29 10:05:00",
                "2026-08-29",
                10,
            ),
            (
                "event-003",
                "device-003",
                "Industrial_Cooler",
                "2026-08-29 10:02:00",
                22.0,
                60.0,
                1.8,
                97.0,
                -16.4,
                34.9,
                "1.0.0",
                "invalid temperature",
                False,
                "2026-08-29 10:05:00",
                "2026-08-29",
                10,
            ),
        ]

        columns = [
            "event_id",
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
            "validation_error",
            "is_valid",
            "ingestion_timestamp",
            "event_date",
            "event_hour",
        ]

        df = spark.createDataFrame(
            data,
            columns,
        )

        df = (
            df
            .withColumn(
                "event_timestamp",
                df.event_timestamp.cast("timestamp"),
            )
            .withColumn(
                "ingestion_timestamp",
                df.ingestion_timestamp.cast("timestamp"),
            )
            .withColumn(
                "event_date",
                df.event_date.cast("date"),
            )
        )

        result = transform_to_silver(df)

        assert result.count() == 2

        event_ids = {
            row["event_id"]
            for row in result.collect()
        }

        assert event_ids == {
            "event-001",
            "event-002",
        }

        device_type = (
            result
            .filter("event_id = 'event-001'")
            .first()["device_type"]
        )

        assert device_type == "industrial_cooler"

        device_id = (
            result
            .filter("event_id = 'event-001'")
            .first()["device_id"]
        )

        assert device_id == "device-001"

    finally:
        spark.stop()