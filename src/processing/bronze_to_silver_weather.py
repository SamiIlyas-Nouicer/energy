"""
processing/bronze_to_silver_weather.py

Reads raw weather JSON from bronze/weather.paris/, resamples hourly
temperature to 30-minute intervals to match the RTE data granularity,
computes heating degree days, and writes a Delta table to silver/weather/.

Run:
    python processing/bronze_to_silver_weather.py
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET           = os.getenv("MINIO_BUCKET",     "energy-lake")

BRONZE_PATH = f"s3a://{BUCKET}/bronze/weather.paris/"
SILVER_PATH = f"s3a://{BUCKET}/silver/weather/"

# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("bronze-to-silver-weather")
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

def transform(df: DataFrame, spark: SparkSession) -> DataFrame:

    # --- 1. Explode hourly arrays from the payload struct ---
    df_exploded = df.select(
        F.explode(
            F.arrays_zip(
                F.col("payload.hourly.time"),
                F.col("payload.hourly.temperature_2m")
            )
        ).alias("row")
    ).select(
        F.col("row.time").alias("time_str"),
        F.col("row.temperature_2m").cast(DoubleType()).alias("temperature_celsius"),
    )

    # --- 2. Parse timestamp ---
    df_exploded = df_exploded.withColumn(
        "timestamp_hour",
        F.to_timestamp("time_str", "yyyy-MM-dd'T'HH:mm")
    ).drop("time_str")

    # --- 3. Expand each hourly row to two 30-minute rows ---
    offsets = spark.createDataFrame([(0,), (30,)], ["offset_min"])

    df_30min = df_exploded.crossJoin(offsets).withColumn(
        "timestamp",
        (
            F.unix_timestamp("timestamp_hour") +
            F.col("offset_min") * 60
        ).cast(TimestampType())
    ).drop("timestamp_hour", "offset_min")

    # --- 4. Drop duplicates ---
    df_30min = df_30min.dropDuplicates(["timestamp"])

    # --- 5. Heating Degree Days ---
    df_30min = df_30min.withColumn(
        "heating_degree_days",
        F.greatest(F.lit(0.0), F.lit(18.0) - F.col("temperature_celsius"))
    )

    # --- 6. Metadata ---
    df_30min = df_30min.withColumn("ingestion_timestamp", F.current_timestamp())
    df_30min = df_30min.withColumn("source", F.lit("open_meteo"))

    return df_30min.orderBy("timestamp")
# ---------------------------------------------------------------------------
# Fallback: if no live weather data yet, create a minimal stub
# ---------------------------------------------------------------------------

def create_stub(spark: SparkSession) -> DataFrame:
    """
    If the weather.paris bronze topic is empty (producers only just started),
    create an empty silver/weather/ Delta table with the correct schema so
    the silver join on Day 3 doesn't fail.
    """
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType as TS

    schema = StructType([
        StructField("timestamp",            TimestampType(), True),
        StructField("temperature_celsius",  DoubleType(),    True),
        StructField("heating_degree_days",  DoubleType(),    True),
        StructField("ingestion_timestamp",  TimestampType(), True),
        StructField("source",               StringType(),    True),
    ])
    print("⚠️  No weather bronze data found — writing empty stub table.")
    return spark.createDataFrame([], schema)

# ---------------------------------------------------------------------------
# Quality checks
# ---------------------------------------------------------------------------

def quality_checks(df: DataFrame) -> None:
    total = df.count()
    print(f"\n--- Quality Checks ---")
    print(f"Total rows       : {total}")

    if total == 0:
        print("⚠️  Table is empty (stub) — will be populated once weather producer runs.")
        return

    null_ts = df.filter(F.col("timestamp").isNull()).count()
    print(f"Null timestamps  : {null_ts}  {'✅' if null_ts == 0 else '❌'}")

    temp_range = df.agg(
        F.min("temperature_celsius").alias("min_t"),
        F.max("temperature_celsius").alias("max_t")
    ).collect()[0]
    min_t, max_t = temp_range["min_t"], temp_range["max_t"]
    in_range = -30 <= (min_t or 0) and (max_t or 0) <= 50
    print(f"Temp range       : {min_t:.1f}°C → {max_t:.1f}°C  {'✅' if in_range else '❌ outside France extremes'}")

    date_range = df.agg(
        F.min("timestamp").alias("min_ts"),
        F.max("timestamp").alias("max_ts")
    ).collect()[0]
    print(f"Date range       : {date_range['min_ts']} → {date_range['max_ts']}")
    print(f"----------------------\n")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Check if bronze weather data exists
    try:
        print("Reading bronze/weather.paris/ ...")
        df_raw = spark.read.json(BRONZE_PATH)
        row_count = df_raw.count()
        print(f"Raw messages found: {row_count}")

        if row_count == 0:
            raise ValueError("Empty")

        df_silver = transform(df_raw, spark)

    except Exception as e:
        print(f"Could not read weather bronze data ({e}) — using stub.")
        df_silver = create_stub(spark)

    quality_checks(df_silver)

    print(f"Writing Delta table to {SILVER_PATH} ...")
    (
        df_silver.write
        .format("delta")
        .mode("overwrite")
        .save(SILVER_PATH)
    )

    print("✅ silver/weather/ written successfully.")

    if df_silver.count() > 0:
        print("\nSample:")
        df_silver.show(5)

    spark.stop()


if __name__ == "__main__":
    main()