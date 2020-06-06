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

    retail_df = (spark.read.parquet('warehouse/retail_cleaned'))


    return retail_df


def transform(retail_df):
    """
    transformations:
        extract color name from description attribute
        select 'Country', 'Quantity', 'UnitPrice' and update product_color by replacing empty value with 'nocolor
        groupBy country and product color
        sum Quantity, UnitePrice as total_price and total_quantity respectively
        Add column avg_spent = total_price/ total_quantity
    :param retail_df:
    :return:
    """
    from pyspark.sql.functions import regexp_extract, col, count, sum, expr, regexp_replace

    extract_str = "(BLACK|WHITE|RED|GREEN|BLUE)"

    transformed_retail = (retail_df.withColumn('product_color', regexp_extract(col("Description"), extract_str, 1))
                          .select('Country', 'Quantity', 'UnitPrice',
                                  regexp_replace(col("product_color"), '^$', "NOCOLOR").alias('product_color'))
                          .groupBy('Country', 'product_color')
                          .agg(sum('Quantity').alias('total_quantity'),
                               sum('UnitPrice').alias('total_price'))
                          .withColumn('avg_spent (dollars)', expr('total_price/total_quantity'))
                          )

    return transformed_retail


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
        .partitionBy('product_color')
        .save('./warehouse/country_color_expanses'))

