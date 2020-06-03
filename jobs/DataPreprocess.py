"""

DataPreprocess.py
--------------------

ETL job to:
EXTRACT initial retail data provided by client from specified location,

TRANSFORM remove/ replace nulls,
          remove errors,
          update schema of dataframe

LOAD (Save) data to specified location for net set of jobs
"""
from dependencies import utility


def extract(spark):
    """Load data csv dataset files.

        :param spark: Spark session object.
        :return: retail park DataFrame.
        """
    retail_df = (spark.read
                 .schema(utility.createRetailSchema())
                 .csv('./input-data/retail/*.csv',
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
    retail_df = retail_df.na.fill('NOVALUE').na.fill(0)

    return retail_df


def load(retail_df):
    """Collect data locally and write to CSV.

    :param retail_df: dataframe to save to db
    :return: None
    """
    # (retail_df
    #  .write
    #  .format('parquet')
    #  .mode('overwrite')
    #  .partitionBy('country')
    #  .bucketBy(3, 'product_color')
    #  .saveAsTable('retailPartitionsParquet'))

    (retail_df
     .repartition(2)
     .write
     .format('json')
     .mode('overwrite')
     .partitionBy('country')
     .save('output-data/warehouse/countryAggJson'))
