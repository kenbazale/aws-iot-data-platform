from pyspark.sql import SparkSession


def create_spark_session(
    app_name: str,
    shuffle_partitions: int = 8,
) -> SparkSession:
    """
    Create a Spark session with our baseline configuration.

    These settings are deliberately conservative for local
    development. Production values will be benchmarked later.
    """

    spark = (
        SparkSession.builder
        .appName(app_name)
        .config(
            "spark.sql.adaptive.enabled",
            "true",
        )
        .config(
            "spark.sql.adaptive.coalescePartitions.enabled",
            "true",
        )
        .config(
            "spark.sql.adaptive.skewJoin.enabled",
            "true",
        )
        .config(
            "spark.sql.shuffle.partitions",
            shuffle_partitions,
        )
        .config(
            "spark.sql.session.timeZone",
            "UTC",
        )
        .getOrCreate()
    )

    return spark