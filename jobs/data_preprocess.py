"""

data_preprocess.py
--------------------

ETL job to:
EXTRACT initial retail data provided by client from specified location,
TRANSFORM in preprocessing stage to identify and remove errors,
LOAD (Save) data to specified location for future Jobs

"""


def extract(spark):
    """Load data from Parquet file format.

        :param spark: Spark session object.
        :return: Spark DataFrame.
        """
    print(spark)
    retail_df = (spark.read.csv('./input-data/retail/*.csv',
                                schema=None,
                                sep=",",
                                inferSchema=True,
                                header=True))

    return retail_df


def transform(retail_df):
    """

    :param retail_df:
    :return:
    """
    from pyspark.sql.functions import regexp_extract, col, count, sum

    extract_str = "(BLACK|WHITE|RED|GREEN|BLUE)"

    transformed_retail = (retail_df.withColumn('product_color', regexp_extract(col("Description"), extract_str, 1))
                                   .groupBy('country', 'product_color')
                                   .agg(count('CustomerID').alias('customer_counts'),
                                        sum('UnitPrice').alias('total_spent')))

    return transformed_retail


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

