"""
processing/bronze_to_silver_generation.py

Reads raw JSON from bronze/historical/, cleans and types every column,
computes derived metrics, and writes a Delta table to silver/generation/.

Run:
    python processing/bronze_to_silver_generation.py
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

BRONZE_PATH = f"s3a://{BUCKET}/bronze/historical/"
SILVER_PATH = f"s3a://{BUCKET}/silver/generation/"

# MW columns that should be cast to double
MW_COLS = [
    "consumption_mw", "forecast_j1_mw", "forecast_j_mw",
    "oil_mw", "coal_mw", "gas_mw", "nuclear_mw",
    "wind_mw", "solar_mw", "hydro_mw", "pumping_mw", "bio_mw",
    "physical_flows_mw",
    "flow_england_mw", "flow_spain_mw", "flow_italy_mw",
    "flow_switzerland_mw", "flow_germany_belgium_mw",
    "oil_tac_mw", "oil_cogen_mw", "oil_other_mw",
    "gas_tac_mw", "gas_cogen_mw", "gas_ccg_mw", "gas_other_mw",
    "hydro_river_mw", "hydro_lake_mw", "hydro_pumped_mw",
    "bio_waste_mw", "bio_biomass_mw", "bio_biogas_mw",
    "battery_charge_mw", "battery_discharge_mw",
    "wind_onshore_mw", "wind_offshore_mw",
]

# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("bronze-to-silver-generation")
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
        # Delta needs this to overwrite existing tables
        .config("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true")
        .getOrCreate()
    )

# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------

def transform(df: DataFrame) -> DataFrame:
    # 1. Cast timestamp
    df = df.withColumn(
        "timestamp",
        F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss")
    )

    # 2. Cast all MW columns to double (they may have come in as strings)
    for col in MW_COLS:
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast(DoubleType()))

    # 3. Drop duplicates on timestamp (keep one reading per 30-min slot)
    df = df.dropDuplicates(["timestamp"])

    # 4. Derived columns
    renewable_cols = ["wind_mw", "solar_mw", "hydro_mw", "bio_mw"]
    existing_renewable = [c for c in renewable_cols if c in df.columns]

    df = df.withColumn(
        "total_production_mw",
        sum(F.coalesce(F.col(c), F.lit(0)) for c in MW_COLS
            if c in df.columns and c not in (
                "consumption_mw", "forecast_j1_mw", "forecast_j_mw",
                "physical_flows_mw", "flow_england_mw", "flow_spain_mw",
                "flow_italy_mw", "flow_switzerland_mw",
                "flow_germany_belgium_mw", "pumping_mw",
                "battery_charge_mw", "battery_discharge_mw",
            ))
    )

    df = df.withColumn(
        "renewable_production_mw",
        sum(F.coalesce(F.col(c), F.lit(0)) for c in existing_renewable)
    )

    df = df.withColumn(
        "renewable_share_pct",
        F.round(
            F.col("renewable_production_mw") / F.col("total_production_mw") * 100,
            2
        )
    )

    # 5. Add ingestion metadata
    df = df.withColumn("ingestion_timestamp", F.current_timestamp())
    df = df.withColumn("source", F.lit("eco2mix_historical"))

    # 6. Sort
    df = df.orderBy("timestamp")

    return df

# ---------------------------------------------------------------------------
# Quality checks
# ---------------------------------------------------------------------------

def quality_checks(df: DataFrame) -> None:
    total = df.count()
    print(f"\n--- Quality Checks ---")
    print(f"Total rows         : {total}")

    null_ts = df.filter(F.col("timestamp").isNull()).count()
    print(f"Null timestamps    : {null_ts}  {'✅' if null_ts == 0 else '❌'}")

    neg_consumption = df.filter(F.col("consumption_mw") < 0).count()
    print(f"Negative consumption: {neg_consumption}  {'✅' if neg_consumption == 0 else '⚠️'}")

    neg_production = df.filter(F.col("total_production_mw") < 0).count()
    print(f"Negative production : {neg_production}  {'✅' if neg_production == 0 else '⚠️'}")

    date_range = df.agg(
        F.min("timestamp").alias("min_ts"),
        F.max("timestamp").alias("max_ts")
    ).collect()[0]
    print(f"Date range         : {date_range['min_ts']} → {date_range['max_ts']}")
    print(f"----------------------\n")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("Reading bronze/historical/ ...")
    df_raw = spark.read.json(BRONZE_PATH)
    print(f"Raw rows: {df_raw.count()}")

    print("Transforming ...")
    df_silver = transform(df_raw)

    quality_checks(df_silver)

    print(f"Writing Delta table to {SILVER_PATH} ...")
    (
        df_silver.write
        .format("delta")
        .mode("overwrite")
        .save(SILVER_PATH)
    )

    print("✅ silver/generation/ written successfully.")
    print("\nSample:")
    df_silver.select(
        "timestamp", "consumption_mw", "nuclear_mw",
        "solar_mw", "renewable_share_pct", "total_production_mw"
    ).show(5)

    spark.stop()


if __name__ == "__main__":
    main()