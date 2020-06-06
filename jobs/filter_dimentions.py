"""

data_preprocess.py
--------------------

ETL job to:
EXTRACT initial retail data provided by client from specified location,
TRANSFORM select specific attributes only from dataset,
LOAD (Save) data to specified location for future Jobs

"""


def extract(spark):
    """Load data from Parquet file format.

        :param spark: Spark session object.
        :return: Spark DataFrame.
        """
    print(spark)
    retail_df = (spark.read.parquet('file:///warehouse/retail_cleaned'))

    return retail_df


def transform(retail_df):
    """

    :param retail_df:
    :return:
    """
    from pyspark.sql.functions import col

    transformed_retail = (retail_df.select(col('InvoiceNo'), col('StockCode'),
                                           col('Quantity'), col('UnitPrice'), col('CustomerID'), col('country')))

    return transformed_retail


def load(retail_df):
    """Collect data locally and write to CSV.

    :param retail_df: dataframe to save to db
    :return: None
    """
    (retail_df
     .coalesce(5)
     .write
     .format('json')
     .mode('overwrite')
     .partitionBy('country')
     .save('file:///warehouse/retail_extracted_features'))


