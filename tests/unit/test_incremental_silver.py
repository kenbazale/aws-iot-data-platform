from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from iot_platform.transforms.silver import transform_to_silver


def test_late_arriving_event_belongs_to_event_partition():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-late-arriving-event")
        .getOrCreate()
    )

    try:
        data = [
            (
                "event-late-001",
                "device-001",
                "Industrial_Cooler",
                "2026-08-28 23:59:00",
                20.5,
                50.0,
                1.8,
                99.0,
                -16.3,
                35.2,
                "1.0.0",
                None,
                True,
                "2026-09-01 14:30:00",
                "2026-08-28",
                23,
            )
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

        schema = StructType(
            [
                StructField("event_id", StringType(), True),
                StructField("device_id", StringType(), True),
                StructField("device_type", StringType(), True),
                StructField("event_timestamp", StringType(), True),
                StructField("temperature", DoubleType(), True),
                StructField("humidity", DoubleType(), True),
                StructField("pressure", DoubleType(), True),
                StructField("battery_level", DoubleType(), True),
                StructField("latitude", DoubleType(), True),
                StructField("longitude", DoubleType(), True),
                StructField("firmware_version", StringType(), True),
                StructField("validation_error", StringType(), True),
                StructField("is_valid", BooleanType(), True),
                StructField("ingestion_timestamp", StringType(), True),
                StructField("event_date", StringType(), True),
                StructField("event_hour", IntegerType(), True),
            ]
        )

        df = spark.createDataFrame(data, schema=schema)

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

        row = result.first()

        assert row["event_id"] == "event-late-001"
        assert str(row["event_date"]) == "2026-08-28"
        assert row["event_hour"] == 23

    finally:
        spark.stop()