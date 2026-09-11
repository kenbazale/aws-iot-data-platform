import logging
from pathlib import Path

from iot_platform.config.settings import load_config
from iot_platform.transforms.silver import transform_to_silver
from iot_platform.utils.spark import create_spark_session


logging.basicConfig(
    level=logging.INFO,
    format=("%(asctime)s | "
        "%(levelname)s | "
        "%(name)s | "
        "%(message)s"),
)

logger = logging.getLogger(__name__)

def main() -> None:
    logger.info("Starting Silver transformation")
    
    config = load_config("configs/dev.yaml")
    
    spark = create_spark_session(config)
    
    bronze_path = Path(config["storage"]["bronze_path"])
    silver_path = Path(config["storage"]["silver_path"])
    
    logger.info(f"Reading Bronze data from {bronze_path}")
    
    try:
        bronze_df = (
            spark.read.parquet(str(bronze_path))
        )
        input_count = bronze_df.count()

        logger.info(
            "Bronze records: %d",
            input_count,
        )

        silver_df = transform_to_silver(
            bronze_df
        )

        output_count = silver_df.count()

        logger.info(
            "Silver records: %d",
            output_count,
        )

        silver_path.mkdir(
            parents=True,
            exist_ok=True,
        )

        logger.info(
            "Writing Silver data to %s",
            silver_path,
        )

        (
            silver_df
            .write
            .mode("overwrite")
            .partitionBy(
                "event_date",
                "event_hour",
            )
            .parquet(str(silver_path))
        )

        logger.info(
            "Silver transformation completed successfully"
        )

    except Exception:
        logger.exception(
            "Silver transformation failed"
        )
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main() 