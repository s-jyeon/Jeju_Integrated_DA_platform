from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.quicksight import QuickSightCreateIngestionOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

# 설정
DATA_SET_ID = "275aa5a4-98e7-4517-b484-7fe6c55099dc"  # QuickSight 데이터 세트 ID
REGION_NAME = "ap-northeast-2"
GLUE_JOB_2 = "jeju_gas_station_price_Info_trans"
GLUE_JOB_3 = "jeju_gas_join"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG 정의
with DAG(
    dag_id="jeju_gas_join_refresh",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval="*/30 * * * *",
    catchup=False,
) as dag:

    # Glue Job 2 실행
    run_glue_job_2 = GlueJobOperator(
        task_id="run_glue_job_2",
        job_name=GLUE_JOB_2,
        region_name=REGION_NAME,
        wait_for_completion=True,
    )

    # Glue Job 3 실행
    run_glue_job_3 = GlueJobOperator(
        task_id="run_glue_job_3",
        job_name=GLUE_JOB_3,
        region_name=REGION_NAME,
        wait_for_completion=True,
    )

    # QuickSight SPICE 새로고침 시작 (고유 ingestion_id 생성)
    quicksight_create_ingestion = QuickSightCreateIngestionOperator(
        task_id="quicksight_create_ingestion",
        data_set_id=DATA_SET_ID,
        ingestion_id=f"gas-price-refresh-{datetime.now().strftime('%Y%m%d%H%M%S')}",  # 동적 ID 생성
        wait_for_completion=True,  # 대기하지 않음
    )

    # 작업 순서 정의
    run_glue_job_2 >> run_glue_job_3 >> quicksight_create_ingestion
