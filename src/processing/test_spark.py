"""
processing/test_spark.py
Quick smoke test — confirms Spark can read from MinIO.
Run: python processing/test_spark.py
"""

from pyspark.sql import SparkSession

MINIO_ENDPOINT   = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

spark = (
    SparkSession.builder
    .appName("smoke-test")
    .master("local[*]")  # runs locally, no need to exec into the container
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

spark.sparkContext.setLogLevel("WARN")

# Read one partition from bronze historical
df = spark.read.json("s3a://energy-lake/bronze/historical/year=2023/month=01/")

print(f"\n✅ Connected to MinIO via S3A")
print(f"   Schema:")
df.printSchema()
print(f"   Row count: {df.count()}")
print(f"   Sample:")
df.select("timestamp", "consumption_mw", "nuclear_mw", "solar_mw").show(3)

spark.stop()