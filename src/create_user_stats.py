import pandas as pd
from src.s3_client import get_s3_client
from datetime import datetime
import io
from botocore.exceptions import ClientError


def read_staging_data(s3_client, bucket_name, key):
    print(f"reading {key}")
    response = s3_client.get_object(
        Bucket=bucket_name,
        Key=key
    )
    body = response["Body"].read()
    buffer = io.BytesIO(body)
    df = pd.read_parquet(buffer)
    return df


def create_user_stats(df):
    user_stats = df.groupby("user_id").agg({
        "id": "count",
        "title_length": "mean"
    })
    user_stats = user_stats.rename(columns={"id": "posts_count", "title_length": "avg_title_length"})
    user_stats = user_stats.reset_index()
    return user_stats


def save_parquet_to_mart(s3_client, user_stats):
    today = datetime.today()
    bucket_name = "mart"
    file_name = f"user_stats_{today.strftime('%Y-%m-%d')}.parquet"
    key = f"user_stats/{today.strftime('%Y/%m/%d')}/{file_name}"
    buffer = io.BytesIO()
    user_stats.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

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
            Body=buffer.getvalue(),
            ContentType="application/parquet"
        )
    except Exception as e:
        print(f"Ошибка при загрузке - {e}")
    print(f"uploaded {file_name} to mart")


def main():
    print("Создание витрины")
    today = datetime.today()
    bucket_name = "staging"
    file_name = f"posts_{today.strftime('%Y-%m-%d')}.parquet"
    key = f"posts/{today.strftime('%Y/%m/%d')}/{file_name}"
    s3_client = get_s3_client()
    print(f"Чтение бакета {bucket_name}")
    df = read_staging_data(s3_client, bucket_name, key)
    user_stats = create_user_stats(df)
    print("Сохраняем витрину")
    save_parquet_to_mart(s3_client, user_stats)


if __name__ == "__main__":
    main()