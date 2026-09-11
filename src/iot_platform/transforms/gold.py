from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


DAILY_METRICS_COLUMNS = [
    "device_id",
    "event_date",
    "event_count",
    "avg_temperature",
    "min_temperature",
    "max_temperature",
    "avg_humidity",
    "min_battery_level",
    "avg_battery_level",
]

LATEST_STATUS_COLUMNS = [
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
]


def build_device_daily_metrics(df: DataFrame) -> DataFrame:
    """
    Build daily telemetry metrics for each device.

    Grain:
        One row per device per event_date.

    Business key:
        (device_id, event_date)

    Input:
        Silver telemetry DataFrame.

    Output:
        Analytics-ready daily metrics.
    """

    return (
        df
        .groupBy(
            "device_id",
            "event_date",
        )
        .agg(
            F.count("*").alias("event_count"),

            F.avg("temperature")
                .alias("avg_temperature"),

            F.min("temperature")
                .alias("min_temperature"),

            F.max("temperature")
                .alias("max_temperature"),

            F.avg("humidity")
                .alias("avg_humidity"),

            F.min("battery_level")
                .alias("min_battery_level"),

            F.avg("battery_level")
                .alias("avg_battery_level"),
        )
        .select(*DAILY_METRICS_COLUMNS)
    )


def get_affected_dates(df: DataFrame) -> list:
    """
    Return the distinct event dates affected by the input DataFrame.

    This is used by incremental Gold processing to determine which
    Gold partitions need to be recalculated.

    Returns:
        Sorted list of Python date objects.
    """

    rows = (
        df
        .select("event_date")
        .where(F.col("event_date").isNotNull())
        .distinct()
        .orderBy("event_date")
        .collect()
    )

    return [row["event_date"] for row in rows]

def write_gold_partitions(
    df: DataFrame,
    output_path: str,
) -> None:
    """
    Write Gold data using dynamic partition overwrite.

    Only event_date partitions present in df are replaced.
    Existing unrelated partitions remain untouched.
    """

    (
        df.write
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("event_date")
        .parquet(output_path)
    )

def write_gold_partitions(
    df: DataFrame,
    output_path: str,
) -> None:
    """
    Write Gold data using dynamic partition overwrite.

    Only event_date partitions present in df are replaced.
    Existing unrelated partitions remain untouched.

    Args:
        df: Gold DataFrame containing event_date.
        output_path: Destination Gold directory.
    """

    (
        df.write
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("event_date")
        .parquet(output_path)
    )


def build_device_latest_status(df: DataFrame) -> DataFrame:
    """
    Build the latest known telemetry status for each device.

    Grain:
        One row per device.

    Business key:
        device_id

    The latest event is determined by event_timestamp.
    """
    window = (
        Window
        .partitionBy("device_id")
        .orderBy(F.col("event_timestamp").desc())
    )

    return (
        df
        .filter(F.col("event_timestamp").isNotNull())
        .withColumn(
            "row_number",
            F.row_number().over(window)
        )
        .filter(F.col("row_number") == 1)
        .drop("row_number")
        .select(*LATEST_STATUS_COLUMNS)
    )