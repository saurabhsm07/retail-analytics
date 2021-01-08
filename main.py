from jobs import data_preprocess, filter_dimentions, country_color_aggregations, max_quantity_rolling, daily_sale
from dependencies import spark
import os


def get_spark_session():
    absolute_path = '/home/maverick/workspace/personal-workspace/spark-workspace/'

    return spark.create_spark_session(app_name='my_etl_job',
                         files=['config/preprocess_config.json'],
                         spark_config={'spark.sql.warehouse.dir': absolute_path + 'retail-analytics/warehouse',
                                       "spark.local.dir": absolute_path + '/retail-analytics/warehouse',
                                       'spark.executor.memory': '1g',
                                       'spark.cores.max': '2'})


def end_spark_session(spark_session):
    spark_session.stop()


def execute_job_0(spark_session):
    # preprocess Job begins
    logger.info('JOB 0: preprocessing initial input data started')
    retail_df = data_preprocess.extract(spark_session)  # extract data from retail data source
    logger.info('JOB 0: extract complete')
    preprocessRetail = data_preprocess.transform(retail_df)
    logger.info('JOB 0: transformation complete')
    data_preprocess.load(preprocessRetail)
    logger.info('JOB 0: load complete')


def execute_job_1(spark_session):
    # preprocess Job begins
    logger.info('JOB 1: preprocessing initial input data started')
    retail_df = country_color_aggregations.extract(spark_session)  # extract data from retail data source
    logger.info('JOB 1: extract complete')
    preprocessed_retail = country_color_aggregations.transform(retail_df)
    logger.info('JOB 1: transformation complete')
    country_color_aggregations.load(preprocessed_retail)
    logger.info('JOB1: load complete')


def execute_job_2(spark_session):
    # filter attribute columns job begins
    logger.info('JOB 2: preprocessing initial input data started')
    retail_df = filter_dimentions.extract(spark_session)  # extract data from retail data source
    logger.info('JOB 2: extract complete')
    preprocessed_retail = filter_dimentions.transform(retail_df)
    logger.info('JOB 2: transformation complete')
    filter_dimentions.load(preprocessed_retail)
    logger.info('JOB2: load complete')


def execute_job_3(spark_session):
    # filter attribute columns job begins
    logger.info('JOB 3 preprocessing initial input data started')
    retail_df = max_quantity_rolling.extract(spark_session)  # extract data from retail data source
    logger.info('JOB 3: extract complete')
    preprocessed_retail = max_quantity_rolling.transform(retail_df)
    logger.info('JOB 3: transformation complete')
    max_quantity_rolling.load(preprocessed_retail)
    logger.info('JOB 3: load complete')

def execute_job_4(spark_session):
    # filter attribute columns job begins
    logger.info('JOB 4 preprocessing initial input data started')
    retail_df = daily_sale.extract(spark_session)  # extract data from retail data source
    logger.info('JOB 4: extract complete')
    preprocessed_retail = daily_sale.transform(retail_df)
    logger.info('JOB 4: transformation complete')
    daily_sale.load(preprocessed_retail)
    logger.info('JOB 4: load complete')


# entry point for PySpark ETL application
if __name__ == '__main__':
    spark_session, logger, configDict = get_spark_session()

    logger.warn('spark session created');

    execute_job_0(spark_session)

    execute_job_1(spark_session)

    execute_job_2(spark_session)

    execute_job_3(spark_session)

    execute_job_4(spark_session)

    end_spark_session(spark_session)
    logger.info("successfully completed spark job")
0