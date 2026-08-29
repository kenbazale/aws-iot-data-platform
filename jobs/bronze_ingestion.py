from pathlib import Path

from pyspark.sql import functions as F

from iot_platform.config.settings import load_config
from iot_platform.quality.checks import validate_telemetry
from iot_platform.schemas.telemetry import TELEMETRY_SCHEMA
from iot_platform.transforms.bronze import enrich_bronze
from iot_platform.utils.logging import (
    configure_logging,
    get_logger,
)
from iot_platform.utils.metrics import PipelineMetrics
from iot_platform.utils.spark import create_spark_session
from iot_platform.ingestion.files import (
    discover_new_files,
)
from iot_platform.ingestion.manifest import (
    ProcessedFileManifest,
)


def main() -> None:

    config = load_config("configs/dev.yaml")

    configure_logging()

    logger = get_logger(__name__)

    spark = create_spark_session(
        app_name=config["spark"]["app_name"],
        shuffle_partitions=config["spark"]["shuffle_partitions"],
    )

    raw_path = Path(
        config["storage"]["raw_path"]
    )

    bronze_path = config["storage"]["bronze_path"]
    dlq_path = "./data/dlq"

    manifest = ProcessedFileManifest(
    "data/state/processed_files.json"
    )

    new_files = discover_new_files(
        str(raw_path),
        manifest,
)

    if not new_files:
        logger.info(
            "No new input files found. Nothing to process."
        )
        spark.stop()
        return

    logger.info(
        "Discovered %d new input file(s)",
        len(new_files),
    )

    for file in new_files:
        logger.info(
            "Input file: %s",
            file,
    )
    
    try:

        logger.info(
            "Starting Bronze ingestion"
        )

        logger.info(
            "Reading raw telemetry from %s",
            raw_path,
        )

        df = (
            spark.read
            .schema(TELEMETRY_SCHEMA)
            .json(
                [str(file) for file in new_files]
            )
        )

        enriched = enrich_bronze(df)

        validated = validate_telemetry(
            enriched
        )

        metrics_row = (
            validated
            .agg(
                F.count("*").alias("total"),
                F.sum(
                    F.when(
                        F.col("is_valid"),
                        1,
                    ).otherwise(0)
                ).alias("valid"),
                F.sum(
                    F.when(
                        ~F.col("is_valid"),
                        1,
                    ).otherwise(0)
                ).alias("invalid"),
            )
            .collect()[0]
        )

        metrics = PipelineMetrics(
            input_records=metrics_row["total"],
            valid_records=metrics_row["valid"] or 0,
            invalid_records=metrics_row["invalid"] or 0,
        )

        metrics.log_summary(logger)

        valid = validated.filter(
            F.col("is_valid") == True
        )

        invalid = validated.filter(
            F.col("is_valid") == False
        )

        if metrics.valid_records > 0:

            logger.info(
                "Writing %d valid records to Bronze",
                metrics.valid_records,
            )

            (
                valid.write
                .mode("append")
                .partitionBy(
                    "event_date",
                    "event_hour",
                )
                .parquet(bronze_path)
            )

        if metrics.invalid_records > 0:

            logger.warning(
                "Writing %d invalid records to DLQ",
                metrics.invalid_records,
            )

            (
                invalid.write
                .mode("append")
                .partitionBy("event_date")
                .json(dlq_path)
            )

        logger.info(
            "Bronze ingestion completed successfully"
        )
        for file in new_files:
            manifest.add(file.name)

        manifest.save()

        logger.info(
            "Updated input manifest with %d file(s)",
            len(new_files),
        )
    except Exception:

        logger.exception(
            "Bronze ingestion failed"
        )

        raise

    finally:

        spark.stop()


if __name__ == "__main__":
    main()