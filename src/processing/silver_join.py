"""
processing/silver_join.py

Joins silver/generation/ and silver/weather/ on timestamp (rounded to the
nearest 30 minutes) and writes the combined table to
silver/energy_with_weather/.

This is the table that dbt, the ML model, and the dashboard all read from.

Run:
    python processing/silver_join.py
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET           = os.getenv("MINIO_BUCKET",     "energy-lake")

GENERATION_PATH = f"s3a://{BUCKET}/silver/generation/"
WEATHER_PATH    = f"s3a://{BUCKET}/silver/weather/"
OUTPUT_PATH     = f"s3a://{BUCKET}/silver/energy_with_weather/"

# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("silver-join")
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
# Helpers
# ---------------------------------------------------------------------------

def round_to_30min(col_name: str) -> F.Column:
    """Round a timestamp column down to the nearest 30-minute boundary."""
    return (
        (F.unix_timestamp(col_name) / 1800).cast("long") * 1800
    ).cast(TimestampType())


# ---------------------------------------------------------------------------
# Join
# ---------------------------------------------------------------------------

def build_joined(df_gen: DataFrame, df_weather: DataFrame) -> DataFrame:

    # Round both sides to 30-minute boundary to guarantee join alignment
    df_gen = df_gen.withColumn(
        "timestamp_30",
        round_to_30min("timestamp")
    )

    df_weather = df_weather.withColumn(
        "timestamp_30",
        round_to_30min("timestamp")
    ).select(
        "timestamp_30",
        "temperature_celsius",
        "heating_degree_days",
    )

    # Left join — keep all generation rows even if weather is missing
    df_joined = df_gen.join(df_weather, on="timestamp_30", how="left")

    # Use the rounded key as the canonical timestamp
    df_joined = df_joined.drop("timestamp").withColumnRenamed("timestamp_30", "timestamp")

    # Data completeness flag
    df_joined = df_joined.withColumn(
        "is_complete",
        F.col("consumption_mw").isNotNull() &
        F.col("nuclear_mw").isNotNull() &
        F.col("temperature_celsius").isNotNull()
    )

    # Add join metadata
    df_joined = df_joined.withColumn("joined_at", F.current_timestamp())

    return df_joined.orderBy("timestamp")


# ---------------------------------------------------------------------------
# Quality checks
# ---------------------------------------------------------------------------

def quality_checks(df: DataFrame) -> None:
    total = df.count()
    print(f"\n--- Quality Checks ---")
    print(f"Total rows          : {total}")

    null_ts = df.filter(F.col("timestamp").isNull()).count()
    print(f"Null timestamps     : {null_ts}  {'✅' if null_ts == 0 else '❌'}")

    complete = df.filter(F.col("is_complete")).count()
    pct = round(complete / total * 100, 1) if total > 0 else 0
    print(f"Complete rows       : {complete} / {total}  ({pct}%)")

    # Weather coverage will be low since we only have 1 week of live data
    weather_matched = df.filter(F.col("temperature_celsius").isNotNull()).count()
    print(f"Rows with weather   : {weather_matched}  ({round(weather_matched/total*100,1)}%)")

    date_range = df.agg(
        F.min("timestamp").alias("min_ts"),
        F.max("timestamp").alias("max_ts")
    ).collect()[0]
    print(f"Date range          : {date_range['min_ts']} → {date_range['max_ts']}")
    print(f"----------------------\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("Reading silver/generation/ ...")
    df_gen = spark.read.format("delta").load(GENERATION_PATH)
    print(f"Generation rows: {df_gen.count()}")

    print("Reading silver/weather/ ...")
    df_weather = spark.read.format("delta").load(WEATHER_PATH)
    print(f"Weather rows   : {df_weather.count()}")

    print("Joining ...")
    df_joined = build_joined(df_gen, df_weather)

    quality_checks(df_joined)

    print(f"Writing Delta table to {OUTPUT_PATH} ...")
    (
        df_joined.write
        .format("delta")
        .mode("overwrite")
        .save(OUTPUT_PATH)
    )

    print("✅ silver/energy_with_weather/ written successfully.")
    print("\nSample:")
    df_joined.select(
        "timestamp",
        "consumption_mw",
        "nuclear_mw",
        "solar_mw",
        "renewable_share_pct",
        "temperature_celsius",
        "heating_degree_days",
        "is_complete"
    ).show(5)

    spark.stop()


if __name__ == "__main__":
    main()