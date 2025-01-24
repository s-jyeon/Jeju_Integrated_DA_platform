from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.quicksight import QuickSightCreateIngestionOperator

# 설정
DATA_SET_ID = "6d060b59-4072-4b26-9349-2af27f6f12b6"  # QuickSight 데이터 세트 ID
REGION_NAME = "ap-northeast-2"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# DAG 정의
with DAG(
    dag_id="jeju_road_hazard_info_refresh",  # DAG 이름 수정
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval="*/2 * * * *",  # 30분마다 실행
    catchup=False,
) as dag:

    # QuickSight SPICE 새로고침 시작 (고유 ingestion_id 생성)
    quicksight_create_ingestion = QuickSightCreateIngestionOperator(
        task_id="quicksight_create_ingestion",
        data_set_id=DATA_SET_ID,
        ingestion_id=f"jeju-road-hazard-info-refresh-{datetime.now().strftime('%Y%m%d%H%M%S')}",  # 동적 ID 생성
        wait_for_completion=True,  # 대기하지 않음
    )
