"""
Script contains utility functions that are required accross multiple jobs
"""


def createRetailSchema():
    """
    method creates and returns a fixed schema for retail dataset
    :return: schema object for retail dataset
    """
    from pyspark.sql.types import StructField, StructType, StringType, LongType, IntegerType, DoubleType, TimestampType

    retailSchema = StructType([
        StructField("InvoiceNo", LongType(), True),
        StructField("StockCode", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("InvoiceDate", TimestampType(), True),
        StructField("UnitPrice", StringType(), True),
        StructField("CustomerID", DoubleType(), True),
        StructField("Country", StringType(), True)
    ])

    return retailSchema
