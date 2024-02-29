from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import random

# Create a list of tuples with fake data
data = [(i, f'name_{i}', f'name_{i}@email.com', f'category_{random.randint(1, 2)}', f'section_{random.randint(1, 5)}')
        for i in range(1000)]

spark = SparkSession.builder \
    .appName("IcebergApp") \
    .getOrCreate()

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("category", StringType(), True),
    StructField("section", StringType(), True)
])

spark.createDataFrame([], schema).writeTo("data_catalog.public.user").create()

# df_2 = df_1
# df_3 = spark.createDataFrame(data, schema)

spark.sql("ALTER TABLE data_catalog.public.user "
          "SET TBLPROPERTIES ('write.metadata.path' = 's3a://my-bucket/public/metadata/')")
spark.sql("ALTER TABLE data_catalog.public.user "
          "SET TBLPROPERTIES ('write.data.path' = 's3a://my-bucket/public/data/')")

df_1 = spark.createDataFrame(data, schema)

df_1.writeTo("data_catalog.public.user").append()

# df_2.writeTo("data_catalog.public.user2").create()
# df_3.writeTo("data_catalog.public.user3").create()

df_dest = spark.table("data_catalog.public.user").show()

print(spark.sql('SHOW TBLPROPERTIES data_catalog.public.user'))
