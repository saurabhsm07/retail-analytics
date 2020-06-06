"""

data_preprocess.py
--------------------

ETL job to:
EXTRACT initial retail data provided by client from specified location,
TRANSFORM analyse products bought of specific color
LOAD (Save) data to specified location for future Jobs

"""


def extract(spark):
    """Load data csv dataset files.

        :param spark: Spark session object.
        :return: retail park DataFrame.
    """

    retail_df = (spark.read.parquet('wasbs:///warehouse/retail_cleaned'))


    return retail_df


def transform(retail_df):
    """

    :param retail_df:
    :return:
    """
    from pyspark.sql.window import Window
    from pyspark.sql.functions import col, date_format, desc, dense_rank, rank, max

    # convert date format on retail_df
    transform_step1 = (retail_df.withColumn('InvoiceDate',
                                            date_format(col("InvoiceDate"), "MM/dd/yyyy H:mm")))

    # window function
    window_function = (Window.partitionBy("CustomerId")
                       .orderBy(desc("Quantity"))
                       .rowsBetween(Window.unboundedPreceding, Window.currentRow))

    # aggregate functions
    max_purchase_quantity = max(col("Quantity")).over(window_function)

    # rank functions
    purchase_dense_rank = dense_rank().over(window_function)
    purchase_rank = rank().over(window_function)

    transformed_df = (retail_df.withColumn('InvoiceDate', date_format(col("InvoiceDate"), "MM/dd/yyyy H:mm"))
                      .where("CustomerId IS NOT NULL")
                      .orderBy("CustomerId")
                      .select(col("CustomerId"),
                              col("InvoiceDate"),
                              col("Quantity"),
                              purchase_rank.alias("quantityRank"),
                              purchase_dense_rank.alias("quantityDenseRank"),
                              max_purchase_quantity.alias("maxPurchaseQuantity")))

    return transformed_df


def load(retail_df):
    """Collect data locally and write to CSV.

    :param retail_df: dataframe to save to db
    :return: None
    """
    (retail_df
     .coalesce(2)
     .write
     .format('json')
     .mode('overwrite')
     .bucketBy(2, 'Quantity')
     .saveAsTable('maximum_rolling_customer'))
