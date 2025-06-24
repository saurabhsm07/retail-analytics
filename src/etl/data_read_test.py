from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a SparkSession
spark = SparkSession.builder \
    .appName("SparkExample") \
    .getOrCreate()

df = spark.read.parquet('s3a://my-bucket/test-data/source/')

print("DataFrame loaded successfully")
print(df.count())
print(df.columns)