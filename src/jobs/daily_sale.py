"""

Daily_expanses.py
--------------------

ETL job to:
EXTRACT initial retail data provided by client from specified location,
TRANSFORM aggregate net sold on a perticular day
LOAD (Save) Save data to mysql database

"""


def extract(spark):
    """Load data csv dataset files.

           :param spark: Spark session object.
           :return: retail park DataFrame.
       """

    retail_df = (spark.read.parquet('./warehouse/retail_cleaned'));

    return retail_df;


def transform(retail_df):
    """

    :param retail_df: retail data of daily items sold
    :return: dataframe of items sold and money made by date
    """
    from pyspark.sql.functions import to_date, col, sum;

    daily_sale = (retail_df.withColumn('invoice_date', to_date(col('InvoiceDate'), 'yyyy-mm-dd'))
                  .withColumn('total_cost', col('UnitPrice') * col('Quantity'))
                  .groupBy('invoice_date')
                  .agg(sum('Quantity').alias('items_sold'),
                       sum('total_cost').alias('gross_profit'))
                  .orderBy(col('invoice_date').desc())
                  )

    return daily_sale;


def load(daily_sale):
    """

    :param daily_sale:  number of  items sold an
    :return:
    """
    (
        daily_sale.write
                  .mode('append').format('jdbc')
                  .option('user', 'root')
                  .option('password', '123456')
                  .option('driver', 'com.mysql.jdbc.Driver')
                  .option('url', 'jdbc:mysql://localhost:3306/products')
                  .option('dbtable', 'daily_sale')
                  .save()
    )
