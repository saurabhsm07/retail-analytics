from jobs import DataPreprocess, FilterDimentions, CountryColorAggregations, MaxQuantityRolling
from dependencies.spark import spark_session
from os import path


def create_spark_session():
    current_dir = path.dirname(path.realpath(__file__))

    return spark_session(app_name='my_etl_job',
                         files=['config/preprocess_config.json'],
                         spark_config={'spark.sql.warehouse.dir': current_dir + '/output-data/warehouse',
                                       'spark.executor.memory': '1g',
                                       'spark.cores.max': '2'})


def end_spark_session(sparkSession):
    sparkSession.stop()

def executeJob0(sparkSession):
    # preprocess Job begins
    logger.info('JOB 0: preprocessing initial input data started')
    retailDf = DataPreprocess.extract(sparkSession)  # extract data from retail data source
    logger.info('JOB 0: extract complete')
    preprocessRetail = DataPreprocess.transform(retailDf)
    logger.info('JOB 0: transformation complete')
    DataPreprocess.load(preprocessRetail)
    logger.info('JOB 0: load complete')

def executeJob1(sparkSession):
    # preprocess Job begins
    logger.info('JOB 1: preprocessing initial input data started')
    retail_df = CountryColorAggregations.extract(spark_session)  # extract data from retail data source
    logger.info('JOB 1: extract complete')
    preprocessed_retail = CountryColorAggregations.transform(retail_df)
    logger.info('JOB 1: transformation complete')
    CountryColorAggregations.load(preprocessed_retail)
    logger.info('JOB1: load complete')


def executeJob2(sparkSession):
    # filter attribute columns job begins
    logger.info('JOB 2: preprocessing initial input data started')
    retail_df = FilterDimentions.extract(spark_session)  # extract data from retail data source
    logger.info('JOB 2: extract complete')
    preprocessed_retail = FilterDimentions.transform(retail_df)
    logger.info('JOB 2: transformation complete')
    FilterDimentions.load(preprocessed_retail)
    logger.info('JOB2: load complete')


def executeJob3(sparkSession):
    # filter attribute columns job begins
    logger.info('JOB 3 preprocessing initial input data started')
    retail_df = MaxQuantityRolling.extract(spark_session)  # extract data from retail data source
    logger.info('JOB 3: extract complete')
    preprocessed_retail = MaxQuantityRolling.transform(retail_df)
    logger.info('JOB 3: transformation complete')
    MaxQuantityRolling.load(preprocessed_retail)
    logger.info('JOB 3: load complete')


# entry point for PySpark ETL application
if __name__ == '__main__':
    sparkSession, logger, configDict = create_spark_session()

    logger.warn('spark session created');

    executeJob0(sparkSession)

    executeJob1(sparkSession)

    executeJob2(sparkSession)

    executeJob3(sparkSession)

    end_spark_session(spark_session)
    logger.info("successfully completed spark job")