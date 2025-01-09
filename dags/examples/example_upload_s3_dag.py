import os
from datetime import datetime

import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator

os.environ["no_proxy"] = "*"

BUCKET_NAME = os.getenv("S3_BUCKET_TEST")
RESULT_PATH = "result/result.txt"


# S3 업로드 함수
def upload_to_s3(file_path, bucket_name, object_name):
    s3 = boto3.client("s3")
    s3.upload_file(file_path, bucket_name, object_name)


# DAG 설정
default_args = {
    "owner": "airflow",
    "retries": 1,
}
dag = DAG(
    "example_upload_to_s3_dag",
    default_args=default_args,
    description="Upload DAG result to S3",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
)


def generate_result_and_upload(**kwargs):
    # 로컬 파일 생성 (예: 처리 결과)
    file_path = "/tmp/result.txt"
    with open(file_path, "w") as f:
        f.write("This is the result data.")

    # S3로 업로드
    bucket_name = BUCKET_NAME  # TODO: 버킷 이름
    object_name = RESULT_PATH  # TODO: 결과 파일 이름
    upload_to_s3(file_path, bucket_name, object_name)


upload_task = PythonOperator(
    task_id="generate_and_upload",
    python_callable=generate_result_and_upload,
    provide_context=True,
    dag=dag,
)

upload_task
