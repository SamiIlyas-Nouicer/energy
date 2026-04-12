from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName('export-silver')
    .master('local[*]')
    .config('spark.jars.packages',
            'org.apache.hadoop:hadoop-aws:3.3.4,'
            'com.amazonaws:aws-java-sdk-bundle:1.12.262,'
            'io.delta:delta-spark_2.12:3.1.0')
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
    .config('spark.hadoop.fs.s3a.endpoint', 'http://localhost:9000')
    .config('spark.hadoop.fs.s3a.access.key', 'minioadmin')
    .config('spark.hadoop.fs.s3a.secret.key', 'minioadmin')
    .config('spark.hadoop.fs.s3a.path.style.access', 'true')
    .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
    .getOrCreate()
)
spark.sparkContext.setLogLevel('WARN')

df = spark.read.format('delta').load('s3a://energy-lake/silver/energy_with_weather/')
print(f'Rows to export: {df.count()}')
df.write.mode('overwrite').parquet('/home/sami/Desktop/energy/src/data/silver_export/')
print('Done')
spark.stop()
