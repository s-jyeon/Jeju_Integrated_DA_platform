from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.quicksight import (
    QuickSightCreateIngestionOperator,
)

# 설정
DATA_SET_ID = "3b93ba39-b550-477d-b13c-ed9044d335c7"  # QuickSight 데이터 세트 ID
INGESTION_ID = "gas-price-refresh-ingestion"  # 고유 SPICE 새로고침 작업 ID
REGION_NAME = "ap-northeast-2"
GLUE_JOB_1 = "gas-price"
GLUE_JOB_2 = "gas-price-transform"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG 정의
with DAG(
    dag_id="quicksight_spice_refresh_no_wait",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",  # 스케줄링 설정 하시면 됩니다.
    catchup=False,
) as dag:
    # Glue Job 1 실행
    run_glue_job_1 = GlueJobOperator(
        task_id="run_glue_job_1",
        job_name=GLUE_JOB_1,
        region_name=REGION_NAME,
        wait_for_completion=True,
    )

    # Glue Job 2 실행
    run_glue_job_2 = GlueJobOperator(
        task_id="run_glue_job_2",
        job_name=GLUE_JOB_2,
        region_name=REGION_NAME,
        wait_for_completion=True,
    )

    # QuickSight SPICE 새로고침 시작
    quicksight_create_ingestion = QuickSightCreateIngestionOperator(
        task_id="quicksight_create_ingestion",
        data_set_id=DATA_SET_ID,
        ingestion_id=INGESTION_ID,
        wait_for_completion=True,  # 대기 할 거임
    )

    # 작업 순서 정의
    run_glue_job_1 >> run_glue_job_2 >> quicksight_create_ingestion
