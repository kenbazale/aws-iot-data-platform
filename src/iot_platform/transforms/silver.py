from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from iot_platform.transforms.deduplication import (
    deduplicate_events,
)


SILVER_COLUMNS = [
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
    "ingestion_timestamp",
    "event_date",
    "event_hour",
]


def transform_to_silver(df: DataFrame) -> DataFrame:
    """
    Transform Bronze telemetry into the Silver data contract.

    Responsibilities:
        1. Keep valid telemetry only.
        2. Deduplicate events.
        3. Normalize selected fields.
        4. Select the explicit Silver schema.

    The function does not perform I/O.
    """

    valid_df = (
        df.filter(F.col("is_valid") == True)
    )

    deduplicated_df = deduplicate_events(
        valid_df
    )

    return (
        deduplicated_df
        .withColumn(
            "device_id",
            F.trim(F.col("device_id")),
        )
        .withColumn(
            "device_type",
            F.lower(F.trim(F.col("device_type"))),
        )
        .select(*SILVER_COLUMNS)
    )