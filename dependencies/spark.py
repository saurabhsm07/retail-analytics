"""
spark.py
~~~~~~~~

Module containing helper functions, method used across multiple jobs
"""

# import __main__

from os import environ, listdir, path
import json
from pyspark import SparkFiles
from pyspark.sql import SparkSession

from dependencies import logging


def create_spark_session(app_name='spark-application', master='yarn',
                         files=[], spark_config={}, jar_packages=[]):
    """
    method creates a spark session object
    :param jar_packages:
    :param app_name: name of the current application
    :param master: master nodes connection details
    :param files: path to files to be placed on each executor
    :param spark_config: dictionary of key-value pair config variables for spark session
    :return: tuple of (create_spark_session,
    """

    spark_builder = (SparkSession
                     .builder
                     .master(master)
                     .enableHiveSupport()
                     .appName(app_name))

    spark_jars_packages = ','.join(list(jar_packages))
    spark_builder.config('spark.jars.packages', spark_jars_packages)

    spark_files = ','.join(list(files))
    spark_builder.config('spark.files', spark_files)

    # add other config params
    for key, val in spark_config.items():
        spark_builder.config(key, val)

    # create session and retrieve Spark logger object
    spark = spark_builder.getOrCreate()

    spark_logger = logging.Log4j(spark)
    # get config file if sent to cluster with --files

    spark_files_dir = SparkFiles.getRootDirectory()

    config_files = [filename
                    for filename in listdir(spark_files_dir)
                    if filename.endswith('config.json')]

    if config_files:
        path_to_config_file = path.join(spark_files_dir, config_files[0])
        with open(path_to_config_file, 'r') as config_file:
            config_dict = json.load(config_file)
        spark_logger.warn('loaded config from ' + config_files[0])
    else:
        spark_logger.warn('no config file found')
        config_dict = None

    return spark, spark_logger, config_dict
