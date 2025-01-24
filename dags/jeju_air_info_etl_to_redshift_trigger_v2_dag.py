import datetime
import boto3
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.operators.quicksight import QuickSightCreateIngestionOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from datetime import timedelta


# s3 버킷의 특정 경로 하위에 파일이 존재하는지 확인하는 함수
def check_files_in_s3(bucket_name, prefix):
    client = boto3.client('s3', region_name='ap-northeast-2')
    aws_hook = AwsBaseHook(aws_conn_id='aws_ip001')

    try:
        response = client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' in response:
            file_list = [obj['Key'] for obj in response['Contents']]
            print(f"Files found: {file_list}")
            return True
        else:
            print("No files found in the specified path.")
            return False
    except Exception as e:
        print(f"Error occurred: {e}")
        return False


# 기본 설정
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'start_date': datetime.datetime(2025, 1, 1),  # 수정된 부분
}

# 설정
data_date = (datetime.datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')  # 가져올 데이터 날짜
DATA_SET_ID = "d4041227-d093-43eb-a6e1-12244cb3b33a"  # QuickSight 데이터 세트 ID
INGESTION_ID = f"jeju_air_info_latest_refresh_ingestion_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"  # 현재 날짜와 시간으로 고유한 ingestionId 생성
REGION_NAME = 'ap-northeast-2'

# DAG 정의
dag = DAG(
    'jeju_air_info_update_to_redshift_trigger_v7',
    default_args=default_args,
    description='Wait for file in S3, run Glue Crawler, then Glue Job',
    schedule_interval=None,  # 외부 트리거로 실행(None) or 주기 설정 (@)
    start_date=datetime.datetime(2025, 1, 1),  # 시작 날짜
    catchup=False,
)

# Glue Job 실행 1.
run_glue_job_task_1 = GlueJobOperator(
    task_id='run_glue_job_1',
    job_name='jeju_air_info_get_api',
    region_name=REGION_NAME,
)

# S3 파일 확인 작업
check_s3_task = PythonOperator(
    task_id='check_files_in_s3',
    python_callable=check_files_in_s3,
    op_kwargs={
        'bucket_name': 'ip-jeju-airflow',
        'prefix': f'data/raw/jeju_air_info/{data_date}/',
    },
    dag=dag,
)

# Glue Job 실행 2 (task_id 수정)
run_glue_job_task_2 = GlueJobOperator(
    task_id='run_glue_job_2',
    job_name='jeju_air_info_transform',
    region_name=REGION_NAME,
)

# S3에서 Redshift로 데이터 복사
REDSHIFT_SCHEMA = 'jeju_weather'  # 적재할 Redshift 스키마
REDSHIFT_TABLE = 'jeju_air_info_latest'  # 적재할 Redshift 테이블 이름
S3_BUCKET = 'ip-jeju-airflow'
S3_KEY = 'data/transformed/jeju_air_info/{{ macros.ds_add(ds, -1) }}/'

s3_to_redshift = S3ToRedshiftOperator(
    task_id='s3_to_redshift_copy',
    schema=REDSHIFT_SCHEMA,
    table=REDSHIFT_TABLE,
    s3_bucket=S3_BUCKET,
    s3_key=S3_KEY,
    copy_options=['FORMAT AS PARQUET'],
    redshift_conn_id='redshift_default',
    dag=dag,
)

# QuickSight SPICE 새로고침 시작
quicksight_create_ingestion = QuickSightCreateIngestionOperator(
    task_id="quicksight_create_ingestion",
    data_set_id=DATA_SET_ID,
    ingestion_id=INGESTION_ID,
    wait_for_completion=True,  # 대기 할 거임
)

# 작업 순서 정의
run_glue_job_task_1 >> check_s3_task >> run_glue_job_task_2 >> s3_to_redshift >> quicksight_create_ingestion


