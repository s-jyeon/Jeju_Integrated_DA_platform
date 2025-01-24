from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.quicksight import QuickSightCreateIngestionOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator


# 설정
INGESTION_ID = f"jeju_domestic_tourist_spot_counts_trend-{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
DATA_SET_ID = "6f2b5d7c-fbf0-415e-b85a-e12e57e92db8"
REGION_NAME = "ap-northeast-2"
GLUE_JOB_1 = "jeju_domestic_tourist_spot_counts_trend"
S3_BUCKET = "ip-jeju-airflow"
S3_KEY = "data/transformed/jeju_domestic_tourist_spot_counts_trend/"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# DAG 정의
with DAG(
    dag_id="jeju_domestic_tourist_spot_counts_trend",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,                    # 스케줄링 설정 하시면 됩니다.
    catchup=False,
) as dag:

    # Glue Job 1 실행
    run_glue_job_1 = GlueJobOperator(
        task_id="run_glue_job_1",
        job_name=GLUE_JOB_1,
        region_name=REGION_NAME,
        wait_for_completion=True,
    )
    # S3 to Redshift load
    load_to_redshift = S3ToRedshiftOperator(
        task_id = 's3_to_redshift',
        schema = 'jeju_tourist',
        table = 'jeju_domestic_tourist_spot_counts_trend',
        s3_bucket = S3_BUCKET,
        s3_key = S3_KEY,
        copy_options = ['FORMAT AS PARQUET'],
        aws_conn_id = 'aws_ip001',
        redshift_conn_id = 'redshift_default'
    )

    # QuickSight SPICE 새로고침 시작 
    quicksight_create_ingestion = QuickSightCreateIngestionOperator(
        task_id="quicksight_create_ingestion",
        data_set_id=DATA_SET_ID,
        ingestion_id=INGESTION_ID,
        wait_for_completion=True,  # 대기 할 거임
    )

    # 작업 순서 정의
    run_glue_job_1 >> load_to_redshift >> quicksight_create_ingestion