from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from datetime import datetime, timedelta
import boto3

from airflow.providers.amazon.aws.operators.quicksight import QuickSightCreateIngestionOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


#기본 설정
default_args = {
    'owner':'airflow',
    'retries':1,
    'retry_delay':timedelta(minutes=5),
    'catchup':False,
    'start_date':datetime(2015,1,7),
}
#DAG 정의
dag = DAG(
    'copys3_to_redshift_jeju_daily_weather_v3',
    default_args=default_args,
    description='Wait for file in S3, run Glue Crawler, then Glue Job',
    schedule_interval=None, # 외부 트리거로 실행(None) or 주기 설정 (@)
    start_date=datetime(2025, 1, 1),  # 시작 날짜
    catchup=False,  # catchup=False는 미실행된 DAG이 이전 날짜로 실행되지 않게 설정
)

# S3에서 Redshift로 데이터를 복사하는 과정
s3_to_redshift = S3ToRedshiftOperator(
    task_id='s3_to_redshift_copy',
    schema='jeju_weather',  # 적재할 Redshift 스키마
    table='jeju_daily_weather',    # 적재할 Redshift 테이블 이름
    s3_bucket='ip-jeju-airflow',
    s3_key='data/transformed/jeju_daily_weather/',
    copy_options=['FORMAT AS PARQUET'],
    #copy_options=['CSV', 'IGNOREHEADER 1'], #csv 파일 load
    redshift_conn_id='redshift_default',
    dag=dag
)

# 작업 순서 정의
s3_to_redshift