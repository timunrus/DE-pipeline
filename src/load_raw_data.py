import requests
import json
from datetime import datetime
from s3_client import get_s3_client
from botocore.exceptions import ClientError

def fetch_posts():
    url = "https://jsonplaceholder.typicode.com/posts"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    return data


def bucket_exists(s3_client, bucket_name):
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except ClientError:
        s3_client.create_bucket(Bucket=bucket_name)


def upload_raw_data(s3_client, bucket_name, data):
    today = datetime.today()
    file_name = f"posts_{today.strftime('%Y-%m-%d')}.json"
    json_string = json.dumps(data)
    s3_client.put_object(
        Bucket=bucket_name,
        Key=f"posts/{today.strftime('%Y/%m/%d')}/{file_name}",
        Body=json_string.encode('utf-8'),
        ContentType="application/json"
    )
    print(f"✅ Файл {file_name} успешно загружен в бакет {bucket_name}")


def main():
    s3_client = get_s3_client()
    bucket_name = "raw"
    data = fetch_posts()
    bucket_exists(s3_client, bucket_name)
    upload_raw_data(s3_client, bucket_name, data)


if __name__ == "__main__":
    main()