from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3PutObjectOperator,
)

# 기본 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

with DAG(
    dag_id="aws_s3_test",
    default_args=default_args,
    description="A simple test DAG for AWS S3",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["aws", "s3"],
) as dag:
    # S3 버킷 생성
    create_bucket = S3CreateBucketOperator(
        task_id="create_s3_bucket",
        bucket_name="my-test-bucket",
        region_name="us-west-2",
    )

    # S3에 파일 업로드
    upload_file = S3PutObjectOperator(
        task_id="upload_to_s3",
        bucket_name="my-test-bucket",
        key="test_file.txt",
        data="This is a test file uploaded via Airflow!",
    )

    # 태스크 순서
    create_bucket >> upload_file
