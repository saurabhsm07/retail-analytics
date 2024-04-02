from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a SparkSession
spark = SparkSession.builder \
    .appName("snowflake-connector-spark") \
    .getOrCreate()

# Create two dummy DataFrames
data1 = [("Alice", 34), ("Bob", 45), ("Charlie", 28)]
data2 = [("Alice", "Engineering"), ("Bob", "Finance"), ("Dave", "HR")]


df1 = spark.createDataFrame(data1, ["name", "age"])
df2 = spark.createDataFrame(data2, ["name", "department"])

# Perform a join operation
joined_df = df1.join(df2, "name")

# Write the result to a local path on the master node
output_path = "s3a://my-bucket/test/csv/"
joined_df.write.csv(output_path, mode="overwrite", header=True)

# Stop the SparkSession
spark.stop()
