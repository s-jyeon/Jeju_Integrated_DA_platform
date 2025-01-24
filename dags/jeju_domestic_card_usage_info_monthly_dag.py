import requests
import time
import json
import csv
from datetime import datetime, timedelta
from io import StringIO

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.operators.quicksight import QuickSightCreateIngestionOperator

# default 설정, airflow dag에 대한 기본 매개변수 집합을 정의
default_args = {
    'owner' : 'airflow',
    'depends_on_past' : False,
    'start_date' : datetime(2025, 1, 6), #2025년 1월 6일부터 대그 시작
    'retries' : 1,
    'retry_delay' : timedelta(minutes=2),
    'provide_context' : True,
    'email' : ['kaitlyn7v@gmail.com'],
    'email_on_failure' : False,
    'email_on_retry' : False
}



# Replace with your app key
your_appkey = "toet3pt6ctje_1cb35bpc3e_e62b7t00"

# API URL
base_url = "https://open.jejudatahub.net/api/proxy/5D5a577taba7tbb71at1b1bt9tatata9/{your_appkey}".replace("{your_appkey}", your_appkey)

# Parameters
start_date = "201609"
end_date = "201812"
limit = 100

# S3 Bucket details
S3_BUCKET_NAME = "ip-jeju-airflow"
S3_KEY = "data/transformed/jeju_domestic_card_usage_info_monthly/"
folder_name = "data/raw/jeju_domestic_card_usage_info_monthly/"
json_file_name = "jeju_domestic_card_usage_info_monthly.json"
csv_file_name = "jeju_domestic_card_usage_info_monthly.csv"

def fetch_api_data(**kwargs):
    # Initialize results
    all_data = []

    # Pagination
    page = 1

    while True:
        params = {
            "startDate" : start_date,
            "endDate" : end_date,
            "number" : page,
            "limit" : 100
        }

        # Make the request
        response = requests.get(base_url, params=params)
        if response.status_code != 200:
            print(f"Failed to fetch data: {response.status_code}")
            break

        # Parse the response JSON
        data = response.json()

        # Check if data exists 데이터 존재 여부 확인
        if "data" not in data or not data["data"]:
            print("No more data to fetch.")
            break

        # Append data to the results
        all_data.extend(data["data"])

        # Print progress
        print(f"Fetced page {page}, total records: {len(all_data)}")

        # Increment page number
        page += 1

        # To avoid overwhelming the server
        time.sleep(1)

# Save data to S3 using S3Hook
    try:
        csv_buffer = StringIO()
        if all_data:
            # Extract keys for CSV header
            keys = all_data[0].keys()
            csv_writer = csv.DictWriter(csv_buffer, fieldnames = keys)
            csv_writer.writeheader()
            csv_writer.writerows(all_data)

         # Save JSON and CSV data to S3 using S3Hook
        s3_hook = S3Hook(aws_conn_id = 'aws_default')

        # Upload JSON file
        s3_hook.load_string(
            string_data=json.dumps(all_data, ensure_ascii=False),
            key = f"{folder_name}{json_file_name}",
            bucket_name = S3_BUCKET_NAME,
            replace = True,
            encoding = 'utf-8'
        )
        print(f"JSON successfully uploaded to s3://{S3_BUCKET_NAME}/{folder_name}{json_file_name}")

        # Upload CSV file
        s3_hook.load_string(
            string_data=csv_buffer.getvalue(),
            key=f"{folder_name}{csv_file_name}",
            bucket_name = S3_BUCKET_NAME,
            replace = True,
            encoding = 'utf-8'
        )
        print(f"CSV successfully uploaded to s3://{S3_BUCKET_NAME}/{folder_name}{csv_file_name}")
    except Exception as e:
        print(f"Failed to upload data to S3 : {str(e)}")

# 설정
DATA_SET_ID = "7d614eb8-bc1b-40e3-a1cf-8a56380fae35"
INGESTION_ID = f"jeju_domestic_card_usage_info_monthly-{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
REGION_NAME = "ap-northeast-2"
GLUE_JOB_1 = "jeju_domestic_card_usage_info_monthly_1"

with DAG(
    dag_id = 'jeju_domestic_card_usage_info_monthly',
    default_args = default_args,
    description = 'API 데이터 수집 후 Redshift load',
    schedule_interval = '@monthly',
    start_date = datetime(2025, 1, 1),
    catchup = False
) as dag:

    # Python task to fetch and upload data
    fetch_and_upload_task = PythonOperator(
        task_id = 'fetch_and_upload_data',
        python_callable = fetch_api_data,
        provide_context = True
    )

    # s3에 원본파일이 떨어지는 것을 감지
    check_s3_file = S3KeySensor(
        task_id = 'check_s3_file',
        bucket_name = S3_BUCKET_NAME,
        bucket_key = folder_name+csv_file_name,
        wildcard_match = True,  # 와일드카드 매칭 사용
        timeout = 3600,  # 타임아웃 설정(초)
        poke_interval = 60 # 확인 주기 (초)
    )

    # Glue ETL Job 실행
    run_glue_etl = GlueJobOperator(
        task_id = 'run_glue_etl',
        job_name = GLUE_JOB_1,
        run_job_kwargs = {
            "WorkerType" : "G.1X",
            "NumberOfWorkers" : 2
            },
        stop_job_run_on_kill = True  # Airflow UI 또는 CLI에서 사용자가 Task를 중단(kill)하면, Glue Job도 즉시 중단
    )
    
    
    # S3 to Redshift load
    load_to_redshift = S3ToRedshiftOperator(
        task_id = 's3_to_redshift',
        schema = 'jeju_consumption',
        table = 'jeju_domestic_card_usage_info_monthly',
        s3_bucket = S3_BUCKET_NAME,
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
	

fetch_and_upload_task >> check_s3_file >> run_glue_etl >> load_to_redshift >> quicksight_create_ingestion