import sys
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofweek, hour, month, round, to_timestamp

# 1. Get process date from Kestra argument
if len(sys.argv) < 2:
    print("ERROR: No date argument provided")
    sys.exit(1)

process_date = sys.argv[1]
print(f"DEBUG: Processing data for {process_date}")

# 2. Define MinIO connection for Pandas (Bypasses Hadoop Java errors)
storage_options = {
    "key": "minio",
    "secret": "minio_password",
    "client_kwargs": {"endpoint_url": "http://minio:9000"},
}

# 3. Read JSON from Bronze bucket using Pandas
input_uri = f"s3://bronze/db/{process_date}/data.json"
print(f"DEBUG: Reading from {input_uri}")
pdf = pd.read_json(input_uri, storage_options=storage_options)

# 4. Initialize Spark for Transformation
spark = (
    SparkSession.builder.appName("PowerConsumptionETL")
    # We still need these for the WRITE step
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minio")
    .config("spark.hadoop.fs.s3a.secret.key", "minio_password")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # These two lines prevent the "60s" crash during the write phase
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
    .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
    .getOrCreate()
)

# 5. Convert Pandas to Spark DataFrame
df = spark.createDataFrame(pdf)

# 6. TRANSFORM
processed_df = (
    df.withColumn("dt", to_timestamp(col("datetime")))
    .withColumn("hour", hour(col("dt")))
    .withColumn("day_of_week", dayofweek(col("dt")))
    .withColumn("month", month(col("dt")))
    .withColumn(
        "total_consumption",
        col("power_consumption_zone1")
        + col("power_consumption_zone2")
        + col("power_consumption_zone3"),
    )
    .select(
        col("dt").alias("timestamp"),
        "hour",
        "day_of_week",
        "month",
        round(col("temperature"), 2).alias("temp"),
        round(col("humidity"), 2).alias("humidity"),
        round(col("total_consumption"), 2).alias("total_kwh"),
        "power_consumption_zone1",
        "power_consumption_zone2",
        "power_consumption_zone3",
    )
)

# 7. WRITE to Silver as Parquet
output_path = f"s3a://silver/db/{process_date}/"
print(f"DEBUG: Writing Parquet to {output_path}")

processed_df.write.mode("overwrite").parquet(output_path)

print("✅ Job completed successfully")
spark.stop()
