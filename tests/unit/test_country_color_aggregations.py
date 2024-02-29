import pytest

from src.jobs import country_color_aggregations
from dependencies import spark


@pytest.fixture(scope="module")
def sparkFix():
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


@pytest.mark.job1
def test_case_1(sparkFix):
    spark_session, logger, configDict = sparkFix
    df = country_color_aggregations.extract(spark_session)
    df.show(1)
    assert 1 == 1