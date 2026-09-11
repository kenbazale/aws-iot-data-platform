import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F


# ---------------------------------------------------------
# Spark / Glue initialization
# ---------------------------------------------------------

args = {
    "JOB_NAME": "IOT-bronze-to-silver"
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

BRONZE_PATH = f"s3://{BUCKET}/bronze/"
SILVER_PATH = f"s3://{BUCKET}/silver/"
# ---------------------------------------------------------
# Read Bronze
# ---------------------------------------------------------

print(f"Reading Bronze data from {BRONZE_PATH}")

bronze_df = spark.read.parquet(BRONZE_PATH)

input_count = bronze_df.count()

print(f"Bronze records: {input_count}")


# ---------------------------------------------------------
# Silver transformation
# ---------------------------------------------------------

silver_df = (
    bronze_df
    .filter(F.col("is_valid") == True)
    .dropDuplicates(["event_id"])
    .withColumn(
        "device_id",
        F.col("device_id")
    )
    .withColumn(
        "device_type",
        F.lower(F.trim(F.col("device_type")))
    )
)

# ---------------------------------------------------------
# Validate
# ---------------------------------------------------------
silver_count = silver_df.count()
print(f"Silver records: {silver_count}")

# ---------------------------------------------------------
# Write Silver
# ---------------------------------------------------------

print(f"Writing Silver data to {SILVER_PATH}")
(
    silver_df
    .write
    .mode("overwrite")
    .partitionBy("event_date", "event_hour")
    .parquet(SILVER_PATH)
)

print("Bronze → Silver completed successfully")

job.commit()