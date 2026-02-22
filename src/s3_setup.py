import boto3
from botocore.client import Config

endpoint = 'http://localhost:9000'
access_key = 'minioadmin'
secret_key = 'minioadmin'

s3_client = boto3.client(
    's3',
    endpoint_url=endpoint,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    config=Config(
        signature_version='s3v4',
        s3={'addressing_style': 'path'}
    ),
    region_name='us-east-1'
)

bucket_name = 'raw'

s3_client.create_bucket(Bucket=bucket_name)
print(f'Бакет {bucket_name} создан')