from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def enrich_bronze(df: DataFrame) -> DataFrame:
    """
    Add ingestion metadata and partition columns.

    event_date and event_hour are derived from EVENT TIME,
    not ingestion time.
    """

    return (
        df
        .withColumn(
            "ingestion_timestamp",
            F.current_timestamp(),
        )
        .withColumn(
            "event_date",
            F.to_date("event_timestamp"),
        )
        .withColumn(
            "event_hour",
            F.hour("event_timestamp"),
        )
    )