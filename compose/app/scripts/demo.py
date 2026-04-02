from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Initialize Spark Session
spark = SparkSession.builder.appName("UserStatsProcessing").getOrCreate()

# 2. Dummy Data: List of users (Name, Email, Age, Height in cm)
data = [
    ("Alice", "alice@example.com", 28, 165),
    ("Bob", "bob@dev.com", 35, 180),
    ("Charlie", "charlie@ops.net", 22, 175),
    ("David", "david@data.com", 35, 190),
    ("Eve", "eve@example.com", 22, 160),
]

# 3. Define Schema and Create DataFrame
columns = ["name", "email", "age", "height"]
df = spark.createDataFrame(data, schema=columns)

print("--- Original Data ---")
df.show()

# 4. Sort by Age (ascending) and then Height (descending)
df_sorted = df.sort(F.col("age").asc(), F.col("height").desc())

print("--- Sorted Data (Age ASC, Height DESC) ---")
df_sorted.show()

# 5. Calculate Statistics
# We'll get the count, average age, and max height
stats_df = df.select(
    F.count("name").alias("total_users"),
    F.avg("age").alias("average_age"),
    F.max("height").alias("max_height"),
    F.min("height").alias("min_height"),
)

print("--- User Statistics ---")
stats_df.show()

# 6. Bonus: Grouped Stats (Average height per age group)
print("--- Avg Height per Age Group ---")
df.groupBy("age").avg("height").show()

# Stop the session
spark.stop()
