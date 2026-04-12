"""
processing/bronze_to_silver_consumption.py

Reads raw RTE consumption JSON from bronze/rte.consumption/, explodes the
nested values array, computes derived energy metrics, and writes two Delta
tables:
  - silver/consumption/          national 15-min readings
  - silver/quality_log/          data quality failures (appended)

Note: the RTE consumption endpoint returns 15-minute intervals and national
totals only (no regional breakdown). Regional data comes from the éCO2mix
historical files already processed in bronze_to_silver_generation.py.

Run:
    python processing/bronze_to_silver_consumption.py
"""

import os
from datetime import datetime, timezone
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, TimestampType, StringType,
    StructType, StructField, BooleanType
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET           = os.getenv("MINIO_BUCKET",     "energy-lake")

BRONZE_PATH      = f"s3a://{BUCKET}/bronze/rte.consumption/"
SILVER_PATH      = f"s3a://{BUCKET}/silver/consumption/"
QUALITY_LOG_PATH = f"s3a://{BUCKET}/silver/quality_log/"

FRANCE_POPULATION = 67_000_000

# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("bronze-to-silver-consumption")
        .master("local[*]")
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
                "io.delta:delta-spark_2.12:3.1.0")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint",          MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",        MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",        MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------

def transform(df: DataFrame) -> DataFrame:
    """
    RTE consumption payload shape (from debug):
    payload.values = array of {start_date, end_date, updated_date, value (MW)}
    Intervals are 15-minute. We explode, parse timestamps, cast, derive metrics.
    """

    # 1. Explode the values array — one row per 15-min reading
    df_exploded = df.select(
        F.col("ingestion_timestamp"),
        F.col("payload.type").alias("reading_type"),   # "REALISED" etc.
        F.explode("payload.values").alias("v")
    ).select(
        "ingestion_timestamp",
        "reading_type",
        F.col("v.start_date").alias("start_date_raw"),
        F.col("v.end_date").alias("end_date_raw"),
        F.col("v.value").cast(DoubleType()).alias("consumption_mw"),
    )

    # 2. Parse timestamps — RTE uses ISO 8601 with timezone offset (+02:00)
    df_exploded = df_exploded.withColumn(
        "timestamp",
        F.to_timestamp("start_date_raw", "yyyy-MM-dd'T'HH:mm:ssXXX")
    ).withColumn(
        "end_timestamp",
        F.to_timestamp("end_date_raw", "yyyy-MM-dd'T'HH:mm:ssXXX")
    ).drop("start_date_raw", "end_date_raw")

    # 3. Drop duplicates on timestamp
    df_exploded = df_exploded.dropDuplicates(["timestamp"])

    # 4. Derived metrics
    # 15-min interval: MW × (15/60) = MWh, divide by 1000 = GWh
    df_exploded = df_exploded.withColumn(
        "consumption_gwh",
        F.round(F.col("consumption_mw") * (15.0 / 60.0) / 1000.0, 6)
    )

    df_exploded = df_exploded.withColumn(
        "consumption_per_capita_kwh",
        F.round(
            (F.col("consumption_gwh") * 1_000_000.0) / FRANCE_POPULATION,
            6
        )
    )

    # 5. Metadata
    df_exploded = df_exploded.withColumn(
        "processed_at", F.current_timestamp()
    ).withColumn(
        "source", F.lit("rte_api_consumption")
    )

    return df_exploded.orderBy("timestamp")

# ---------------------------------------------------------------------------
# Quality checks + log to Delta
# ---------------------------------------------------------------------------

def quality_checks(df: DataFrame, spark: SparkSession) -> DataFrame:
    """
    Run assertions and write failures to silver/quality_log/.
    Returns the cleaned DataFrame (failures removed).
    """
    job_name = "bronze_to_silver_consumption"
    run_ts   = datetime.now(timezone.utc).isoformat()
    total    = df.count()

    print(f"\n--- Quality Checks ---")
    print(f"Total rows        : {total}")

    failures = []

    # Rule 1: no null timestamps
    null_ts = df.filter(F.col("timestamp").isNull())
    null_ts_count = null_ts.count()
    print(f"Null timestamps   : {null_ts_count}  {'✅' if null_ts_count == 0 else '❌'}")
    if null_ts_count > 0:
        failures.append(("null_timestamp", null_ts_count))

    # Rule 2: consumption >= 0
    neg = df.filter(F.col("consumption_mw") < 0)
    neg_count = neg.count()
    print(f"Negative MW       : {neg_count}  {'✅' if neg_count == 0 else '❌'}")
    if neg_count > 0:
        failures.append(("negative_consumption_mw", neg_count))

    # Rule 3: consumption in realistic France range (10,000 – 120,000 MW)
    out_range = df.filter(
        (F.col("consumption_mw") < 10_000) | (F.col("consumption_mw") > 120_000)
    )
    out_range_count = out_range.count()
    print(f"Out-of-range MW   : {out_range_count}  {'✅' if out_range_count == 0 else '⚠️'}")
    if out_range_count > 0:
        failures.append(("consumption_out_of_range", out_range_count))

    # Date range
    date_range = df.agg(
        F.min("timestamp").alias("min_ts"),
        F.max("timestamp").alias("max_ts")
    ).collect()[0]
    print(f"Date range        : {date_range['min_ts']} → {date_range['max_ts']}")
    print(f"----------------------\n")

    # Write failures to quality_log Delta table
    if failures:
        log_schema = StructType([
            StructField("job_name",    StringType(),    False),
            StructField("run_ts",      StringType(),    False),
            StructField("rule",        StringType(),    False),
            StructField("fail_count",  DoubleType(),    False),
            StructField("passed",      BooleanType(),   False),
        ])
        log_rows = [(job_name, run_ts, rule, float(count), False)
                    for rule, count in failures]
        log_df = spark.createDataFrame(log_rows, log_schema)

        (
            log_df.write
            .format("delta")
            .mode("append")
            .save(QUALITY_LOG_PATH)
        )
        print(f"⚠️  {len(failures)} quality failure(s) logged to silver/quality_log/")
    else:
        # Log a passing run too so the log is complete
        log_schema = StructType([
            StructField("job_name",    StringType(),    False),
            StructField("run_ts",      StringType(),    False),
            StructField("rule",        StringType(),    False),
            StructField("fail_count",  DoubleType(),    False),
            StructField("passed",      BooleanType(),   False),
        ])
        log_df = spark.createDataFrame(
            [(job_name, run_ts, "all_checks", 0.0, True)],
            log_schema
        )
        (
            log_df.write
            .format("delta")
            .mode("append")
            .save(QUALITY_LOG_PATH)
        )
        print("✅ All quality checks passed — logged to silver/quality_log/")

    # Return clean data (drop null timestamps, keep rest with warnings)
    return df.filter(F.col("timestamp").isNotNull())

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("Reading bronze/rte.consumption/ ...")
    df_raw = spark.read.json(BRONZE_PATH)
    raw_count = df_raw.count()
    print(f"Raw messages: {raw_count}")

    if raw_count == 0:
        print("⚠️  No consumption data in bronze yet — "
              "leave the producer running and rerun this job later.")
        spark.stop()
        return

    print("Transforming ...")
    df_silver = transform(df_raw)
    df_silver = quality_checks(df_silver, spark)

    print(f"Writing Delta table to {SILVER_PATH} ...")
    (
        df_silver.write
        .format("delta")
        .mode("overwrite")
        .save(SILVER_PATH)
    )

    print("✅ silver/consumption/ written successfully.")
    print("\nSample:")
    df_silver.select(
        "timestamp",
        "consumption_mw",
        "consumption_gwh",
        "consumption_per_capita_kwh",
        "reading_type",
    ).show(5)

    spark.stop()


if __name__ == "__main__":
    main()