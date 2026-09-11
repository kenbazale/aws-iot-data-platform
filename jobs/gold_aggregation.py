import logging

from iot_platform.config.settings import load_config
from iot_platform.transforms.gold import (
    build_device_daily_metrics,
    build_device_latest_status,
    get_affected_dates,
    write_gold_partitions,
)
from iot_platform.utils.spark import create_spark_session


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)



def main():
    config = load_config("configs/dev.yaml")

    spark = create_spark_session(
        app_name=f"{config['spark']['app_name']}-gold"
    )

    try:
        logger.info("Starting Gold aggregation")

        silver_path = config["storage"]["silver_path"]
        gold_path = config["storage"]["gold_path"]

        logger.info("Reading Silver data from %s", silver_path)

        silver_df = spark.read.parquet(silver_path)

        silver_count = silver_df.count()
        logger.info("Silver records available: %d", silver_count)

        affected_dates = get_affected_dates(silver_df)

        logger.info("Affected dates: %s", affected_dates)

        if not affected_dates:
            logger.info("No affected dates found. Nothing to process.")
            return

        affected_silver_df = silver_df.filter(
            silver_df.event_date.isin(affected_dates)
        )

        affected_count = affected_silver_df.count()

        logger.info(
            "Silver records processed: %d",
            affected_count,
        )

        gold_df = build_device_daily_metrics(
            affected_silver_df
        )

        gold_count = gold_df.count()

        logger.info(
            "Gold daily metrics produced: %d",
            gold_count,
        )

        logger.info("Writing Gold daily metrics")

        write_gold_partitions(
            gold_df,
            gold_path,
        )

        latest_status_df = build_device_latest_status(
            affected_silver_df
        )

        latest_status_count = latest_status_df.count()

        logger.info(
            "Latest device statuses produced: %d",
            latest_status_count,
        )

        latest_status_path = f"{gold_path}/device_latest_status"

        logger.info(
            "Writing latest device status to %s",
            latest_status_path,
        )

        latest_status_df.write \
            .mode("overwrite") \
            .parquet(latest_status_path)

        logger.info("Gold aggregation completed successfully") 

    except Exception:
        logger.exception("Gold aggregation failed")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()