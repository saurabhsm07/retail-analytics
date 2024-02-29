# Follows: https://iceberg.apache.org/docs/latest/spark-procedures/#table-migration


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
import random
import pyspark.sql.functions as F

# SETUP SCRIPT
data = [(i,
         f'name_{i}',
         f'name_{i}@email.com',
         ['cat_1', 'cat_1', 'cat_1', 'cat_1', 'cat_1', 'cat_2', None][random.randint(0, 6)],
         ['section_1', 'section_2', 'section_3', 'section_4', 'section_5'][random.randint(0, 2)])
        for i in range(10000)]

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

output_path = "s3a://my-bucket/old/user/data/"
metadata_path = "s3a://my-bucket/old/user/metadata/"

df = spark.createDataFrame(data, schema)


# write as parquet file
df.write.partitionBy('category', 'section').mode("overwrite").parquet(output_path)

# MY CHANGES

spark.createDataFrame([], schema).writeTo(
    "data_catalog.test_db.user").partitionedBy("category","section").createOrReplace()

spark.sql(f"ALTER TABLE data_catalog.test_db.user "
          f"SET TBLPROPERTIES ('write.metadata.path' = '{metadata_path}')")
spark.sql(f"ALTER TABLE data_catalog.test_db.user "
          f"SET TBLPROPERTIES ('write.data.path' = '{output_path}')")
# #
#
add_files_script = ("CALL data_catalog.system.add_files(table => 'data_catalog.test_db.user', "
                    f"source_table => '`parquet`.`{output_path}`')")

spark.sql(add_files_script)
print(spark.sql('SHOW TBLPROPERTIES data_catalog.test_db.user').show())
