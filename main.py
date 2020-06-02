from jobs import data_preprocess, filter_dimentions, country_color_agg, max_quantity_bought
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

    retail_df = country_color_agg.extract(spark_session)  # extract data from retail data source
    logger.info('JOB 1: extract complete')

    preprocessed_retail = country_color_agg.transform(retail_df)
    logger.info('JOB 1: transformation complete')

    country_color_agg.load(preprocessed_retail)
    logger.info('JOB1: load complete')

    # filter attribute columns job begins
    logger.info('JOB 2: preprocessing initial input data started')

    retail_df = filter_dimentions.extract(spark_session)  # extract data from retail data source
    logger.info('JOB 2: extract complete')

    preprocessed_retail = filter_dimentions.transform(retail_df)
    logger.info('JOB 2: transformation complete')

    filter_dimentions.load(preprocessed_retail)
    logger.info('JOB2: load complete')

    # filter attribute columns job begins
    logger.info('JOB 3 preprocessing initial input data started')

    retail_df = max_quantity_bought.extract(spark_session)  # extract data from retail data source
    logger.info('JOB 3: extract complete')

    preprocessed_retail = max_quantity_bought.transform(retail_df)
    logger.info('JOB 3: transformation complete')

    max_quantity_bought.load(preprocessed_retail)
    logger.info('JOB 3: load complete')

