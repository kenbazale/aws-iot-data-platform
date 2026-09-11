from pathlib import Path
from typing import List, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


Partition = Tuple[str, int]


def get_affected_partitions(
    df: DataFrame,
) -> List[Partition]:
    """
    Return the unique event_date/event_hour partitions
    affected by the supplied DataFrame.

    Only the small set of partition identifiers is collected
    to the driver. Individual telemetry records are never
    collected.
    """

    rows = (
        df.select(
            "event_date",
            "event_hour",
        )
        .where(
            F.col("event_date").isNotNull()
            & F.col("event_hour").isNotNull()
        )
        .distinct()
        .collect()
    )

    return [
        (str(row["event_date"]), row["event_hour"])
        for row in rows
    ]


def partition_path(
    base_path: Path,
    event_date: str,
    event_hour: int,
) -> Path:
    """
    Build the filesystem path for an event-time partition.
    """

    return (
        base_path
        / f"event_date={event_date}"
        / f"event_hour={event_hour}"
    )