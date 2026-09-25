import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)


# ---------------------------------------------------------------------------
# Job arguments
# ---------------------------------------------------------------------------

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "SOURCE_PATH", "BRONZE_PATH", "DLQ_PATH"],
)


# ---------------------------------------------------------------------------
# Spark / Glue initialization
# ---------------------------------------------------------------------------

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SOURCE_PATH = args["SOURCE_PATH"]
BRONZE_PATH = args["BRONZE_PATH"]
DLQ_PATH = args["DLQ_PATH"]


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

TELEMETRY_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("pressure", DoubleType(), True),
        StructField("battery_level", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("firmware_version", StringType(), True),
    ]
)


# ---------------------------------------------------------------------------
# Read raw streaming data
# ---------------------------------------------------------------------------

print(f"Reading raw streaming data from {SOURCE_PATH}")

raw_df = (
    spark.read
    .schema(TELEMETRY_SCHEMA)
    .option("recursiveFileLookup", "true")
    .option("multiLine", "true")
    .json(SOURCE_PATH)
)


# ---------------------------------------------------------------------------
# Normalize and enrich
# ---------------------------------------------------------------------------

normalized_df = (
    raw_df
    .withColumn(
        "event_timestamp",
        F.to_timestamp("event_timestamp"),
    )
    .withColumn(
        "device_id",
        F.trim(F.col("device_id")),
    )
    .withColumn(
        "device_type",
        F.lower(F.trim(F.col("device_type"))),
    )
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


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

valid_condition = (
    F.col("event_id").isNotNull()
    & F.col("device_id").isNotNull()
    & F.col("event_timestamp").isNotNull()
    & F.col("temperature").isNotNull()
    & F.col("humidity").isNotNull()
    & F.col("battery_level").isNotNull()
)

valid_df = normalized_df.filter(valid_condition)

invalid_df = normalized_df.filter(~valid_condition)


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

valid_df = valid_df.dropDuplicates(["event_id"])


# ---------------------------------------------------------------------------
# Write Bronze
# ---------------------------------------------------------------------------

print(f"Writing Bronze data to {BRONZE_PATH}")

(
    valid_df.write
    .mode("append")
    .partitionBy("event_date", "event_hour")
    .parquet(BRONZE_PATH)
)


# ---------------------------------------------------------------------------
# Write DLQ
# ---------------------------------------------------------------------------

if not invalid_df.rdd.isEmpty():

    print(f"Writing invalid records to {DLQ_PATH}")

    (
        invalid_df.write
        .mode("append")
        .partitionBy("event_date")
        .json(DLQ_PATH)
    )


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

input_count = raw_df.count()
valid_count = valid_df.count()
invalid_count = invalid_df.count()

print(
    "Bronze metrics | "
    f"input={input_count} | "
    f"valid={valid_count} | "
    f"invalid={invalid_count}"
)


job.commit()