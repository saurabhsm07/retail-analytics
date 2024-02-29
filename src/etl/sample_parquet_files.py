from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import random

# Create a list of tuples with fake data
data = [(i, f'name_{i}', f'name_{i}@email.com', f'category_{random.randint(1, 2)}', f'section_{random.randint(1, 5)}')
        for i in range(10000)]

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("category", StringType(), True),
    StructField("section", StringType(), True)
])

# Create a SparkSession
spark = SparkSession.builder \
    .appName("FakerDataFrameExample") \
    .getOrCreate()

# Create Spark DataFrame from schema and data
df = spark.createDataFrame(data, schema)
output_path = "s3a://my-bucket/test/parquet/user_data/"
# write as parquet file
df.write.partitionBy("category", "section").mode("overwrite").parquet(output_path)
