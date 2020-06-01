from jobs import data_preprocess
from dependencies.spark import spark_session
from os import path


def create_spark_session():
    current_dir = path.dirname(path.realpath(__file__))

    return spark_session(app_name='my_etl_job',
                         files=['config/preprocess_config.json'],
                         spark_config={'spark.sql.warehouse.dir': current_dir + '/output-data/warehouse',
                                       'spark.executor.memory': '1g',
                                       'spark.cores.max': '2'})


def end_spark_session(spark_session):
    spark_session.stop()


# entry point for PySpark ETL application
if __name__ == '__main__':
    spark_session, logger, config_dict = create_spark_session()

    logger.warn('spark session created');

    # preprocess Job begins
    logger.info('JOB 1: preprocessing initial input data started')

    retail_df = data_preprocess.extract(spark_session)  # extract data from retail data source
    logger.info('JOB 1: extract complete')

    preprocessed_retail = data_preprocess.transform(retail_df)
    logger.info('JOB 1: transformation complete')

    data_preprocess.load(preprocessed_retail)
    logger.info('JOB1: load complete')
