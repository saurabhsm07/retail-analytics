from dependencies import spark

import pytest


@pytest.fixture(scope="module")
def spark():
    absolute_path = '/home/maverick/workspace/personal-workspace/spark-workspace/'
    spark_session, logger, configDict = spark.create_spark_session(app_name='my_etl_job',
                                                                   files=['config/preprocess_config.json'],
                                                                   spark_config={'spark.sql.warehouse.dir': absolute_path + 'retail-analytics/warehouse',
                                                                                 "spark.local.dir": absolute_path + '/retail-analytics/warehouse',
                                                                                 'spark.executor.memory': '1g',
                                                                                 'spark.cores.max': '2'})
    
    logger.info("spark session started")
    yield spark_session, logger, configDict
    spark_session.stop()
    logger.info("spark session stopped")
