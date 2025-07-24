import json
import os
from pyspark.sql.functions import udf, col, lit, struct, transform, create_map, to_json
import bz2
from utilities.spark_session import SparkManager
from pyspark import SparkFiles

from utilities.log import get_logger
from pyspark.sql.functions import struct


logger = get_logger(__name__)

def upload_partition_to_api(iterator, api_endpoint):
    import requests
    try:
        export_data = list(iterator)
        logger.info(f"Processing partition with {len(export_data)} rows")
        logger.info(f"Data {export_data[:5]}")  # Log first 5 rows for debugging
        data = '\n'.join([row.json for row in export_data])

        # metadata = {'description': f"{type} Push",'extensions':{'urnType':type}}

        # files = [('metadata',('metadata.json',json.dumps(metadata),'application/json')),
        #                         ('data',('rows.bz2',bz2.compress(data.encode('utf-8')),'application/x-bzip2')),
        #                         ('gdpr',('gdpr',b'false','application/octet-stream'))]
        
        # Upload as a .txt file as well
        response = requests.post(
            url=api_endpoint,
            files=[('file', (f'{os.getpid()}_rows.txt', data.encode('utf-8'), 'application/txt'))]
        )
        # logger.info(f"TXT upload status: {response_txt.status_code}, response: {response_txt.text}")
        # response = requests.post(url=api_endpoint, files=[('file',(f'{os.getpid()}_rows.bz2',bz2.compress(data.encode('utf-8')),'application/x-bzip2'))])

        result = {
        'partition_id': os.getpid(),
        'status_code': response.status_code,
        'response': response.json(),
        'row_count': len(export_data)
        }
        return iter([result])
        
    except Exception as e:
        return iter([{'error': str(e), 'row_count': len(list(iterator))}])


def main():
    src_data_path = "s3a://my-bucket/test-data/"
    spark_manager = SparkManager("process_csv_and_post")
    spark = spark_manager.create_session()

    df_src = spark.read.option('delimiter',',').option('header',True).csv(src_data_path, header=True).selectExpr('id AS urn',"split(segments,'[|]') AS segments").limit(100000)
    df_seg_as_map = df_src.withColumn(
        'seg',
        transform(
            col('segments'),
            lambda x: create_map(lit('id'), x))
        )
    
    df_json_struct = df_seg_as_map.withColumn(
        'json',
        to_json(
            struct(
                col('urn'),
                col('seg')
            )
        )
    )

    logger.info(df_json_struct.select('json').show(truncate=False))

    result_rdd = df_json_struct.select('json').repartition(10).rdd.mapPartitions(
        lambda iterator: upload_partition_to_api(iterator, api_endpoint="http://host.docker.internal:5000/upload")
    )

    logger.info(result_rdd.collect())

if __name__ == "__main__":
    import os
    print(f"Current working directory: {os.getcwd()}")
    main()