from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from datetime import timedelta

import sys
sys.path.append("/opt/airflow")

from src.load_raw_data import main as load_raw
from src.transform_posts import main as transform
from src.create_user_stats import main as mart

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
    dag_id="data_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args
) as dag:

    task1 = PythonOperator(
        task_id="load_raw_data",
        python_callable=load_raw
    )

    task2 = PythonOperator(
        task_id="transform_posts",
        python_callable=transform
    )

    task3 = PythonOperator(
        task_id="mart",
        python_callable=mart
    )

    task1 >> task2 >> task3