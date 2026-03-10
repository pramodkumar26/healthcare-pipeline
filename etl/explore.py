from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("healthcare-eda") \
    .getOrCreate()

df = spark.read.csv(
    "gs://healthcare-pipeline-raw-data/raw/medicare_2022.csv",
    header=True,
    inferSchema=True
)

print("=== SHAPE ===")
print(f"Rows: {df.count()}, Columns: {len(df.columns)}")

print("\n=== COLUMNS & TYPES ===")
df.printSchema()

print("\n=== SAMPLE ROWS ===")
df.show(5, truncate=False)

print("\n=== NULL COUNTS ===")
from pyspark.sql.functions import col, sum as spark_sum
df.select([spark_sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]).show(truncate=False)

print("\n=== BASIC STATS ===")
df.describe().show(truncate=False)

spark.stop()