from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def validate_telemetry(df: DataFrame) -> DataFrame:
    """
    Add a validation status and failure reason.

    This function does not discard bad records.
    Invalid records are marked so the caller can route them
    to the appropriate destination.
    """

    return (
        df
        .withColumn(
            "validation_error",
            F.when(
                F.col("event_id").isNull(),
                F.lit("missing_event_id"),
            )
            .when(
                F.col("device_id").isNull(),
                F.lit("missing_device_id"),
            )
            .when(
                F.col("event_timestamp").isNull(),
                F.lit("missing_event_timestamp"),
            )
            .when(
                ~F.col("temperature").between(-100.0, 150.0),
                F.lit("temperature_out_of_range"),
            )
            .when(
                ~F.col("humidity").between(0.0, 100.0),
                F.lit("humidity_out_of_range"),
            )
            .when(
                F.col("pressure") <= 0,
                F.lit("pressure_invalid"),
            )
            .when(
                ~F.col("battery_level").between(0.0, 100.0),
                F.lit("battery_out_of_range"),
            )
        )
        .withColumn(
            "is_valid",
            F.col("validation_error").isNull(),
        )
    )