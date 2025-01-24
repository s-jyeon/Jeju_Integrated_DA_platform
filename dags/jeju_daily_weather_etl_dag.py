from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from datetime import datetime, timedelta
import boto3

from airflow.providers.amazon.aws.operators.quicksight import QuickSightCreateIngestionOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook



#s3 버킷의 특정 경로 하위에 파일이 존재하느닞 확인하는 함수
def check_files_in_s3(bucket_name, prefix):
    """
    S3 버킷의 특정 경로 하위에 파일이 존재하는지 확인하는 함수.
    
    :param bucket_name: S3 버킷 이름
    :param prefix: 확인할 S3 경로 (예: 'data/jeju-daily-weather/')
    :return: 경로 하위에 파일이 존재하면 True, 없으면 False
    """
    client = boto3.client('s3', region_name='ap-northeast-2')
    aws_hook = AwsBaseHook(aws_conn_id='aws_default')
    
    try:
        # S3 경로에 해당하는 객체 목록 가져오기
        response = client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        
        # 'Contents' 키가 존재하면 해당 경로에 파일이 존재
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






#기본 설정
default_args = {
    'owner':'airflow',
    'retries':1,
    'retry_delay':timedelta(minutes=5),
    'catchup':False,
    'start_date':datetime(2015,1,7),
}

# Glue 크롤러를 실행하는 함수
def run_glue_crawler(crawler_name):
    client = boto3.client('glue', region_name='ap-northeast-2')
    aws_hook = AwsBaseHook(aws_conn_id='aws_ip001')
    response = client.start_crawler(Name=crawler_name)
    print(f"Started Glue Crawler: {crawler_name}")
    return response

# Glue Job을 실행하는 함수
def run_glue_job(job_name, script_args=None):
    client = boto3.client('glue', region_name='ap-northeast-2')
    aws_hook = AwsBaseHook(aws_conn_id='aws_ip001')
    response = client.start_job_run(
        JobName=job_name,
        Arguments=script_args or {}
    )
    print(f"Started Glue Job: {job_name}, Response: {response}")
    return response


#DAG 정의
dag = DAG(
    'jeju_daily_weather_quarterly_V3',
    default_args=default_args,
    description='Wait for file in S3, run Glue Crawler, then Glue Job',
    schedule_interval='@quarterly',# 외부 트리거로 실행(None) or 주기 설정 (@)
    catchup=False
)

# Glue Job 실행 1.
run_glue_job_task_1 = PythonOperator(
    task_id='run_glue_job_1',
    python_callable=run_glue_job,
    op_args=['jeju_daily_weather'],  # Glue Job 이름
    op_kwargs={'script_args': {'--source_bucket': 'ip-jeju-airflow',
                               '--target_bucket': 'ip-jeju-airflow'}}
)

check_s3_task = PythonOperator(
    task_id='check_files_in_s3',
    python_callable=check_files_in_s3,
    op_kwargs={
        'bucket_name': 'ip-jeju-airflow',
        'prefix': 'data/raw/jeju_daily_weather/',
    },
    dag=dag,
)

# Glue Crawler 실행 1.
run_glue_crawler_task_1 = PythonOperator(
    task_id='run_glue_crawler_1',  # 고유한 task_id 사용
    python_callable=run_glue_crawler,
    op_kwargs={'crawler_name': 'jeju_daily_weather'},  # 키워드 인자 전달
    dag=dag,
)

# Glue Job 실행 2.
run_glue_job_task_2 = PythonOperator(
    task_id='run_glue_job_2',
    python_callable=run_glue_job,
    op_args=['jeju_daily_weather_transform'],  # Glue Job 이름
    op_kwargs={'script_args': {'--source_bucket': 'ip-jeju-airflow',
                               '--target_bucket': 'ip-jeju-airflow'}}
)

# Glue Crawler 실행 2.
run_glue_crawler_task_2 = PythonOperator(
    task_id='run_glue_crawler_2',  # 고유한 task_id 사용
    python_callable=run_glue_crawler,
    op_kwargs={'crawler_name': 'jeju_daily_weather_transform'},  # 키워드 인자 전달
    dag=dag,
)



# S3에서 Redshift로 데이터를 복사하는 과정
s3_to_redshift = S3ToRedshiftOperator(
    task_id='s3_to_redshift_copy',
    schema='jeju_weather',  # 적재할 Redshift 스키마
    table='jeju_daily_weather',    # 적재할 Redshift 테이블 이름
    s3_bucket='ip-jeju-airflow',
    s3_key='/data/transformed/jeju_daily_weather/',
    copy_options=['FORMAT AS PARQUET'],
    #copy_options=['CSV', 'IGNOREHEADER 1'], #csv 파일 load
    redshift_conn_id='redshift_default',
    dag=dag
)





DATA_SET_ID = "5ff6287a-18aa-40fe-a883-f4130702ada3"  # QuickSight 데이터 세트 ID
import datetime

# 현재 날짜와 시간으로 고유한 ingestionId 생성
INGESTION_ID = f"jeju-daily-weather-refresh-ingestion-{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"


 # QuickSight SPICE 새로고침 시작 
quicksight_create_ingestion = QuickSightCreateIngestionOperator(
    task_id="quicksight_create_ingestion",
    data_set_id=DATA_SET_ID,
    ingestion_id=INGESTION_ID,
    wait_for_completion=True,  # 대기 할 거임
    )

# 작업 순서 정의
run_glue_job_task_1 >> check_s3_task >> run_glue_crawler_task_1 >> run_glue_job_task_2 >> run_glue_crawler_task_2 >> s3_to_redshift >> quicksight_create_ingestion
