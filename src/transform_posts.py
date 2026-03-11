from s3_client import get_s3_client
import json
from datetime import datetime
import pandas as pd
import io
from botocore.exceptions import ClientError


def read_raw_data(s3_client, bucket_name, key):
    response = s3_client.get_object(
        Bucket=bucket_name,
        Key=key
    )
    body = response["Body"].read()
    data = json.loads(body)
    return data


def transform_data(data):
    df = pd.DataFrame(data)
    print(df.shape)
    print(df.head())

    df = df[["id", "userId", "title"]]
    df = df.rename(columns={"userId": "user_id"})

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine='pyarrow')
    buffer.seek(0)

    return df, buffer


def upload_parquet_to_staging(s3_client, parquet_buffer):
    today = datetime.today()
    bucket_name = "staging"
    file_name = f"posts_{today.strftime('%Y-%m-%d')}.parquet"
    key = f"posts/{today.strftime('%Y/%m/%d')}/{file_name}"
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print("Bucket exists")
    except ClientError:
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"Bucket ({bucket_name}) created")
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=parquet_buffer.getvalue(),
            ContentType="application/parquet"
        )
        print(f"✅ Файл {file_name} успешно загружен в бакет {bucket_name}")
    except Exception as e:
        print(f"Ошибка при загрузке - {e}")


def main():
    s3_client = get_s3_client()
    bucket_name = 'raw'
    today = datetime.today()
    file_name = f"posts_{today.strftime('%Y-%m-%d')}.json"
    key = f"posts/{today.strftime('%Y/%m/%d')}/{file_name}"
    data = read_raw_data(s3_client, bucket_name, key)
    df, parquet_buffer = transform_data(data)
    upload_parquet_to_staging(s3_client, parquet_buffer)


if __name__ == "__main__":
    main()
