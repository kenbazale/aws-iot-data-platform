from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ---------------------------------------------------------
# Glue / Spark initialization
# ---------------------------------------------------------

args = {
    "JOB_NAME": "iot-silver-to-gold"
}

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

BUCKET = "aws-iot-data-platform-821667315166"

SILVER_PATH = f"s3://{BUCKET}/silver/"
GOLD_PATH = f"s3://{BUCKET}/gold/"


# ---------------------------------------------------------
# Read Silver
# ---------------------------------------------------------

print(f"Reading Silver data from {SILVER_PATH}")

silver_df = spark.read.parquet(SILVER_PATH)

silver_count = silver_df.count()

print(f"Silver records: {silver_count}")


# ---------------------------------------------------------
# Gold: daily device metrics
# ---------------------------------------------------------

daily_metrics = (
    silver_df
    .groupBy("device_id", "event_date")
    .agg(
        F.count("*").alias("event_count"),
        F.avg("temperature").alias("avg_temperature"),
        F.min("temperature").alias("min_temperature"),
        F.max("temperature").alias("max_temperature"),
        F.avg("humidity").alias("avg_humidity"),
        F.min("battery_level").alias("min_battery_level"),
        F.avg("battery_level").alias("avg_battery_level"),
    )
)

daily_count = daily_metrics.count()

print(f"Gold daily metric records: {daily_count}")


# ---------------------------------------------------------
# Write Gold daily metrics
# ---------------------------------------------------------

DAILY_GOLD_PATH = GOLD_PATH

print(f"Writing daily metrics to {DAILY_GOLD_PATH}")

(
    daily_metrics
    .write
    .mode("overwrite")
    .partitionBy("event_date")
    .parquet(DAILY_GOLD_PATH)
)


# ---------------------------------------------------------
# Gold: latest device status
# ---------------------------------------------------------

window = (
    Window
    .partitionBy("device_id")
    .orderBy(
        F.col("event_timestamp").desc(),
        F.col("event_id").desc(),
    )
)

latest_status = (
    silver_df
    .filter(F.col("event_timestamp").isNotNull())
    .withColumn(
        "_row_number",
        F.row_number().over(window)
    )
    .filter(F.col("_row_number") == 1)
    .drop("_row_number")
    .select(
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
    )
)

latest_count = latest_status.count()

print(f"Latest device statuses: {latest_count}")


# ---------------------------------------------------------
# Write latest device status
# ---------------------------------------------------------

LATEST_STATUS_PATH = f"{GOLD_PATH}device_latest_status/"

print(f"Writing latest device status to {LATEST_STATUS_PATH}")

(
    latest_status
    .write
    .mode("overwrite")
    .parquet(LATEST_STATUS_PATH)
)


print("Silver -> Gold completed successfully")

job.commit()