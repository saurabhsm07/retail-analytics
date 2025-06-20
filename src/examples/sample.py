from pyspark.sql import SparkSession
from pyspark.sql import Row

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SimpleTableExample") \
    .getOrCreate()

# Sample data
data = [
    Row(id=1, name="Alice", age=30),
    Row(id=2, name="Bob", age=25),
    Row(id=3, name="Charlie", age=35)
]

# Create DataFrame
df = spark.createDataFrame(data)

# Register DataFrame as a SQL temporary view
df.createOrReplaceTempView("people")

# Query the table
result = spark.sql("SELECT name, age FROM people")

# Show results
result.show()

# Stop Spark session
spark.stop()
