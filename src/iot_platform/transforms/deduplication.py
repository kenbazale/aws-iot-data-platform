from pyspark.sql import DataFrame


def deduplicate_events(df: DataFrame) -> DataFrame:
    """
    Remove duplicate telemetry events using event_id.

    event_id is treated as the unique identifier for a telemetry
    event.

    This protects downstream tables from duplicate events when
    the same source files are processed more than once.
    """

    return df.dropDuplicates(["event_id"])