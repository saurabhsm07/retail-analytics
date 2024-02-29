import pandas as pd
import numpy as np
import pytest

from src.jobs import data_preprocess
from dependencies import spark


@pytest.fixture(scope="module")
def spark_fixture():
    absolute_path = '/home/maverick/workspace/personal-workspace/spark-workspace/'
    spark_session, logger, configDict = spark.create_spark_session(app_name='my_etl_job',
                                                                   files=['config/preprocess_config.json'],
                                                                   spark_config={
                                                                       'spark.sql.warehouse.dir': absolute_path + 'retail-analytics/warehouse',
                                                                       "spark.local.dir": absolute_path + '/retail-analytics/warehouse',
                                                                       'spark.executor.memory': '1g',
                                                                       'spark.cores.max': '2'})

    logger.info("spark session started")
    yield spark_session, logger, configDict
    spark_session.stop()
    logger.info("spark session stopped")


@pytest.mark.job_0
def test_case_1(spark_fixture):
    spark_session, logger, configDict = spark_fixture
    retail_df = data_preprocess.extract(spark_session, './input-data/test-data/retail.csv')
    retail_df = data_preprocess.transform(retail_df).toPandas()

    retail_pdf = (pd.read_csv('./input-data/test-data/retail.csv',
                              sep=",",
                              header=0))

    retail_pdf['InvoiceDate'] = pd.to_datetime(retail_df['InvoiceDate'])
    retail_pdf = (retail_df.assign(InvoiceYear=retail_df['InvoiceDate'].dt.year,
                                   InvoiceMonth=retail_df['InvoiceDate'].dt.month,
                                   InvoiceDay=retail_df['InvoiceDate'].dt.day)
                  .loc[:, ["InvoiceNo", "StockCode", "InvoiceYear", "InvoiceMonth", "InvoiceDay"]]
                  .sort_values(axis=0,
                               by=["InvoiceYear", "InvoiceMonth", "InvoiceDay"],
                               ascending=[False, False, False]
                               )
                  )

    assert retail_df.loc[20, "StockCode"] == retail_pdf.loc[20, "StockCode"]
