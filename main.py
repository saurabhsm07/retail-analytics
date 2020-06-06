from jobs import data_preprocess, filter_dimentions, country_color_aggregations, max_quantity_rolling
from dependencies import spark
import os


def get_spark_session():
    absolute_path = 'wasbs://retail-cluster-2020-06-06t04-18-37-166z@retailclusterhdistorage.blob.core.windows.net/'

    return spark.create_spark_session(app_name='my_etl_job',
                         files=['config/preprocess_config.json'],
                         spark_config={'spark.sql.warehouse.dir': 'warehouse',
                                       "spark.local.dir":  'warehouse',
                                       'spark.executor.instances': 2,
                                       'spark.executor.memory': '1g',
                                       'spark.executor.cores': 2
                                       })


def end_spark_session(spark_session):
    spark_session.stop()


def execute_job0(spark_session):
    # preprocess Job begins
    logger.info('JOB 0: preprocessing initial input data started')
    retail_df = data_preprocess.extract(spark_session)  # extract data from retail data source
    logger.info('JOB 0: extract complete')
    preprocessRetail = data_preprocess.transform(retail_df)
    logger.info('JOB 0: transformation complete')
    data_preprocess.load(preprocessRetail)
    logger.info('JOB 0: load complete')


def executeJob1(spark_session):
    # preprocess Job begins
    logger.info('JOB 1: preprocessing initial input data started')
    retail_df = country_color_aggregations.extract(spark_session)  # extract data from retail data source
    logger.info('JOB 1: extract complete')
    preprocessed_retail = country_color_aggregations.transform(retail_df)
    logger.info('JOB 1: transformation complete')
    country_color_aggregations.load(preprocessed_retail)
    logger.info('JOB1: load complete')


def executeJob2(spark_session):
    # filter attribute columns job begins
    logger.info('JOB 2: preprocessing initial input data started')
    retail_df = filter_dimentions.extract(spark_session)  # extract data from retail data source
    logger.info('JOB 2: extract complete')
    preprocessed_retail = filter_dimentions.transform(retail_df)
    logger.info('JOB 2: transformation complete')
    filter_dimentions.load(preprocessed_retail)
    logger.info('JOB2: load complete')


def executeJob3(spark_session):
    # filter attribute columns job begins
    logger.info('JOB 3 preprocessing initial input data started')
    retail_df = max_quantity_rolling.extract(spark_session)  # extract data from retail data source
    logger.info('JOB 3: extract complete')
    preprocessed_retail = max_quantity_rolling.transform(retail_df)
    logger.info('JOB 3: transformation complete')
    max_quantity_rolling.load(preprocessed_retail)
    logger.info('JOB 3: load complete')


# entry point for PySpark ETL application
if __name__ == '__main__':
    spark_session, logger, configDict = get_spark_session()

    logger.warn('spark session created');

    execute_job0(spark_session)

    executeJob1(spark_session)

    executeJob2(spark_session)

    executeJob3(spark_session)

    end_spark_session(spark_session)
    logger.info("successfully completed spark job")
