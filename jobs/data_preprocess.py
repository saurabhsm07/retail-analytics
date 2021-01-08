"""

data_preprocess.py
--------------------

ETL job to:
EXTRACT initial retail data provided by client from specified location,

TRANSFORM remove/ replace nulls,
          remove errors,
          update schema of dataframe

LOAD (Save) data to specified location for net set of jobs
"""
from dependencies import utility


def extract(spark, file_path = './input-data/retail/*.csv'):
    """Load data csv dataset files.

        :param file_path: path to the directory/ files for the extract stage
        :param spark: Spark session object.
        :return: retail park DataFrame.
        """
    retail_df = (spark.read
                 .schema(utility.create_retail_schema())
                 .csv(file_path,
                      schema=None,
                      sep=",",
                      header=True,
                      mode='permissive'))

    return retail_df


def transform(retail_df):
    """

    :param retail_df: data frame to be preprocessed
    :return: cleaned/ preprocessed data frame for further execution
    """
    from pyspark.sql.functions import year, month, dayofmonth, col

    retail_df = (retail_df.na.fill(0)
                 .na.fill('NOVALUE')
                 .withColumn('invoiceYear', year(col('InvoiceDate')))
                 .withColumn('invoiceMonth', month(col('InvoiceDate')))
                 .withColumn('invoiceDay', dayofmonth('InvoiceDate')))

    return retail_df


def load(retail_df):
    """Collect data locally and write to CSV.

    :param retail_df: dataframe to save to db
    :return: None
    """

    (retail_df
     .coalesce(2)
     .write
     .format('parquet')
     .mode('overwrite')
     .partitionBy('invoiceYear', 'invoiceMonth', 'invoiceDay')
     .bucketBy(5, 'Country')
     .saveAsTable('retail_cleaned'))


