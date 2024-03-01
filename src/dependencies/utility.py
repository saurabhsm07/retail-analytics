"""
Script contains utility functions that are required across multiple jobs
"""


def create_retail_schema():
    """
    method creates and returns a fixed schema for retail dataset
    :return: schema object for retail dataset
    """
    from pyspark.sql.types import StructField, StructType, StringType, LongType, IntegerType, DoubleType, TimestampType

    retail_schema = StructType([
        StructField("InvoiceNo", LongType(), True),
        StructField("StockCode", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("InvoiceDate", TimestampType(), True),
        StructField("UnitPrice", StringType(), True),
        StructField("CustomerID", DoubleType(), True),
        StructField("Country", StringType(), True)
    ])

    return retail_schema
