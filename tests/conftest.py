import pandas as pd
import json

import pytest
from faker import Faker
import boto3
import io
from pyspark.sql import SparkSession


def generate_test_pii_data(num_records=1000):
    data = []
    fake = Faker()

    for _ in range(10000):
        first_name = fake.first_name()
        last_name = fake.last_name()
        name_json = json.dumps({'first_name': first_name, 'last_name': last_name})
        address_json = json.dumps({
            'address1': fake.street_address(),
            'city': fake.city(),
            'state': fake.state(),
            'zip': fake.zipcode()
        })
        data.append({
            'id': fake.uuid4()[:7],
            'phone_number': fake.phone_number(),
            'name': name_json,
            'address': address_json,
            'email': fake.email()
        })

    return pd.DataFrame(data)

def generate_test_pii_data_to_s3_parquet(bucket_name, object_key, 
                                         aws_access_key_id='test', 
                                         aws_secret_access_key='test', 
                                         aws_session_token='test', 
                                         region_name='us-east-1'):

    
    df = generate_test_pii_data()
    
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token,
        region_name=region_name
    )
    s3.upload_fileobj(buffer, bucket_name, object_key)
    return f's3://{bucket_name}/{object_key}'

@pytest.fixture(scope='session')
def s3_test_data():
    s3_path = []
    def _generate_test_data(bucket_name, object_key):
        
        s3_path.append((bucket_name, object_key))
        return generate_test_pii_data_to_s3_parquet(bucket_name, object_key)
    
    yield _generate_test_data
    
    # # Cleanup if necessary
    # s3 = boto3.client('s3')
    # s3.delete_object(Bucket=s3_path[0][0], Key=s3_path[0][1]) if len(s3_path) > 0 else None

def get_spark_session():
    
    return SparkSession.builder \
        .appName("TestSparkSession") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()