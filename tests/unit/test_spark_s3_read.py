import pytest
import boto3
import io


@pytest.mark.spark_s3_read
def test_identity_resolution(s3_test_data, spark_session):
    src_path = s3_test_data('my-bucket', 'test-data/source/test_pii_data.parquet')
    target_path = s3_test_data('my-bucket', 'test-data/target/test_pii_data.parquet')



    # Extract bucket and key
    def split_s3_path(s3_path):
        parts = s3_path.replace('s3://','').split('/', 1)
        return parts[0], parts[1]

    spark_session.read.parquet(src_path.replace('s3:/','s3a:/')).count() > 0
    spark_session.read.parquet(target_path).count() > 0

    