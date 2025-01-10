import io
import json
import logging
import os
from datetime import datetime, timedelta

import boto3
import numpy as np
import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.utils.task_group import TaskGroup
from botocore.exceptions import ClientError

# 로컬 환경 설정: 모든 코드에 존재해야 함!!
os.environ["no_proxy"] = "*"

SECRET_NAME = "ip-jeju-jeju-data-hub-key"
REGION_NAME = "ap-northeast-2"

BUCKET_RAWS = "ip-jeju-raws"
BUCKET_TRANSFORMED = "ip-jeju-transformed"

DATA_SET_ID = "3b93ba39-b550-477d-b13c-ed9044d335c7"  # QuickSight 데이터 세트 ID
INGESTION_ID = "gas-price-refresh-ingestion"  # 고유 SPICE 새로고침 작업 ID
GLUE_JOB_1 = "gas-price"
GLUE_JOB_2 = "gas-price-transform"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "jeju_car_sharing_company_locations",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
)

with TaskGroup("bronze_layer", dag=dag) as bronze_layer:

    def get_secret_key_from_secrets_manager(**context):
        secret_key = None
        try:
            client = boto3.client(
                service_name="secretsmanager",
                region_name=REGION_NAME,
            )
            get_secret_value_response = client.get_secret_value(SecretId=SECRET_NAME)
            secret_key = json.loads(get_secret_value_response["SecretString"])["KEY"]
            logging.info(f"SECRET KEY: {secret_key}")
            context["task_instance"].xcom_push(key="secret_key", value=secret_key)
        except ClientError as e:
            raise e

    get_secret_key_from_secrets_manager_task = PythonOperator(
        task_id="get_secret_key_from_secrets_manager_task",
        python_callable=get_secret_key_from_secrets_manager,
        dag=dag,
    )

    def get_data_from_api(**context):
        secret_key = context["task_instance"].xcom_pull(
            task_ids="bronze_layer.get_secret_key_from_secrets_manager_task",
            key="secret_key",
        )
        logging.info(secret_key)
        if not secret_key:
            raise ValueError("Failed to retrieve secret_key from XCom")

        try:
            req_url = f"https://open.jejudatahub.net/api/proxy/88D0ba0a01a08D081tt8aDba21aabt28/{secret_key}"
            res: requests.Response = requests.get(
                req_url,
                timeout=30,
            )
            res.raise_for_status()  # HTTP 에러 발생 시 예외 발생

            res_data = json.loads(res.content)["data"]
            logging.info(f"Data retrieved: {len(res_data)} records")
            context["task_instance"].xcom_push(key="res_data", value=res_data)
        except requests.exceptions.RequestException as e:
            logging.error(f"API request failed: {e}")
            raise e

    get_data_from_api_task = PythonOperator(
        task_id="get_data_from_api_task",
        python_callable=get_data_from_api,
        retries=3,
        retry_delay=timedelta(seconds=10),
        dag=dag,
    )

    def prepare_csv_data(**context):
        res_data = context["task_instance"].xcom_pull(
            task_ids="bronze_layer.get_data_from_api_task",
            key="res_data",
        )
        if not res_data:
            raise ValueError("No data received from API task")

        df = pd.DataFrame(res_data)
        csv_data: str = df.to_csv(
            index=False, encoding="utf-8"
        )  # CSV 데이터를 문자열로 반환
        context["task_instance"].xcom_push(key="csv_data", value=csv_data)

    prepare_csv_data_task = PythonOperator(
        task_id="prepare_csv_data_task",
        python_callable=prepare_csv_data,
        dag=dag,
    )

    save_data_s3_raws_task = S3CreateObjectOperator(
        task_id="save_data_s3_raws_task",
        s3_bucket=BUCKET_RAWS,  # TODO: 버킷 이름
        s3_key="jeju_car_sharing_company_locations/jeju_car_sharing_company_locations.csv",  # TODO: S3 객체 키 (저장 경로 및 파일 이름)
        data="{{ task_instance.xcom_pull(key='csv_data', task_ids='bronze_layer.prepare_csv_data_task') }}",
        replace=True,
        dag=dag,
    )

    (
        get_secret_key_from_secrets_manager_task
        >> get_data_from_api_task
        >> prepare_csv_data_task
        >> save_data_s3_raws_task
    )


with TaskGroup("silver_layer", dag=dag) as silver_layer:

    def fetch_data_from_s3_raws(**context):
        s3_bucket = BUCKET_RAWS
        s3_key = (
            "jeju_car_sharing_company_locations/jeju_car_sharing_company_locations.csv"
        )

        try:
            # S3 클라이언트 생성
            s3_client = boto3.client("s3", region_name=REGION_NAME)

            # S3 객체 다운로드
            response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
            csv_content = (
                response["Body"].read().decode("utf-8")
            )  # CSV 데이터를 문자열로 읽기

            # CSV 데이터를 데이터프레임으로 변환
            df = pd.read_csv(io.StringIO(csv_content))
            logging.info(f"Fetched {len(df)} rows from S3")
        except ClientError as e:
            logging.error(f"Failed to fetch data from S3: {e}")
            raise e

        context["task_instance"].xcom_push(
            key="fetched_data", value=df.to_dict(orient="records")
        )

    fetch_data_from_s3_raws_task = PythonOperator(
        task_id="fetch_data_from_s3_raws_task",
        python_callable=fetch_data_from_s3_raws,
        dag=dag,
    )

    # 1. Validate(유효성 검사)
    def validate_data(**context):
        data = context["task_instance"].xcom_pull(
            task_ids="silver_layer.fetch_data_from_s3_raws_task",
            key="fetched_data",
        )
        df = pd.DataFrame(data)

        # 데이터 타입 확인
        if not np.issubdtype(df["longitude"].dtype, np.number) or not np.issubdtype(
            df["latitude"].dtype, np.number
        ):
            raise ValueError("longitude and latitude must be numeric")

        context["task_instance"].xcom_push(
            key="validated_data", value=df.to_dict(orient="records")
        )

    validate_data_task = PythonOperator(
        task_id="validate_data_task",
        python_callable=validate_data,
        dag=dag,
    )

    # 2. Clean(데이터 정리)
    def clean_data(**context):
        data = context["task_instance"].xcom_pull(
            task_ids="silver_layer.validate_data_task",
            key="validated_data",
        )
        df = pd.DataFrame(data)

        # 중복 제거
        df.drop_duplicates(inplace=True)
        # 불필요한 공백 제거
        df["placeName"] = df["placeName"].str.strip()

        context["task_instance"].xcom_push(
            key="cleaned_data", value=df.to_dict(orient="records")
        )

    clean_data_task = PythonOperator(
        task_id="clean_data_task",
        python_callable=clean_data,
        dag=dag,
    )

    # 3. Standardize (표준화)
    def standardize_data(**context):
        data = context["task_instance"].xcom_pull(
            task_ids="silver_layer.clean_data_task",
            key="cleaned_data",
        )
        df = pd.DataFrame(data)

        context["task_instance"].xcom_push(
            key="standardized_data",
            value=df.to_dict(orient="records"),
        )

    standardize_data_task = PythonOperator(
        task_id="standardize_data_task",
        python_callable=standardize_data,
        dag=dag,
    )

    def save_parquet_to_s3(**context):
        data = context["task_instance"].xcom_pull(
            task_ids="silver_layer.standardize_data_task",
            key="standardized_data",
        )
        if not data:
            raise ValueError("No data found to save as Parquet")

        # 데이터프레임으로 변환
        df = pd.DataFrame(data)

        # Parquet 데이터 메모리에 저장
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)

        # S3에 업로드
        s3_key = "jeju_car_sharing_company_locations/jeju_car_sharing_company_locations.parquet"  # 저장 경로 및 파일 이름

        try:
            s3_client = boto3.client("s3", region_name=REGION_NAME)
            s3_client.upload_fileobj(buffer, BUCKET_TRANSFORMED, s3_key)
            logging.info(
                f"Parquet file successfully uploaded to s3://{BUCKET_TRANSFORMED}/{s3_key}"
            )
        except ClientError as e:
            logging.error(f"Failed to upload Parquet file to S3: {e}")
            raise e

    save_parquet_to_s3_task = PythonOperator(
        task_id="save_parquet_to_s3_task",
        python_callable=save_parquet_to_s3,
        dag=dag,
    )

    def create_glue_catalog(**context):
        database_name = "ip_jeju_transformed_db"  # Glue Catalog 데이터베이스 이름
        table_name = "jeju_car_sharing_company_locations"  # Glue Catalog 테이블 이름
        s3_bucket = "ip-jeju-transformed"
        s3_key = "jeju_car_sharing_company_locations/jeju_car_sharing_company_locations.parquet"  # S3 경로

        glue_client = boto3.client("glue", region_name=REGION_NAME)

        try:
            glue_client.create_database(
                DatabaseInput={
                    "Name": database_name,
                    "Description": "Jeju car sharing locations database for ETL pipeline.",
                }
            )
            logging.info(f"Database '{database_name}' created or already exists.")
        except glue_client.exceptions.AlreadyExistsException:
            logging.warning(f"Database '{database_name}' already exists.")
        except Exception as e:
            logging.error(f"Database creation failed: {e}")
            raise e

        try:
            # 테이블 생성
            glue_client.create_table(
                DatabaseName=database_name,
                TableInput={
                    "Name": table_name,
                    "StorageDescriptor": {
                        "Columns": [
                            {"Name": "placeName", "Type": "string"},
                            {"Name": "category", "Type": "string"},
                            {"Name": "addressJibun", "Type": "string"},
                            {"Name": "addressDoro", "Type": "string"},
                            {"Name": "longitude", "Type": "double"},
                            {"Name": "latitude", "Type": "double"},
                            {"Name": "placeUrl", "Type": "double"},
                        ],
                        "Location": f"s3://{s3_bucket}/{s3_key}",
                        "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                        "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                        "SerdeInfo": {
                            "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                            "Parameters": {"serialization.format": "1"},
                        },
                    },
                    "TableType": "EXTERNAL_TABLE",
                    "Parameters": {"classification": "parquet"},
                },
            )
            logging.info(
                f"Table '{table_name}' successfully created in '{database_name}'."
            )
        except glue_client.exceptions.AlreadyExistsException:
            logging.warning(
                f"Table '{table_name}' already exists in '{database_name}'."
            )
            # 테이블이 존재하는 경우 세부 정보 로깅
            existing_table = glue_client.get_table(
                DatabaseName=database_name, Name=table_name
            )
            logging.info(f"Existing table details: {existing_table}")
        except Exception as e:
            logging.error(f"Table creation failed: {e}")
            raise e

    create_glue_catalog_task = PythonOperator(
        task_id="create_glue_catalog_task",
        python_callable=create_glue_catalog,
        dag=dag,
    )

    (
        fetch_data_from_s3_raws_task
        >> validate_data_task
        >> clean_data_task
        >> standardize_data_task
        >> save_parquet_to_s3_task
        >> create_glue_catalog_task
    )

with TaskGroup("gold_layer", dag=dag) as gold_layer:

    def aggregate_data(**kwargs):
        logging.info("Aggregating data...")
        # 데이터 집계 작업 추가

    aggregate_data_task = PythonOperator(
        task_id="aggregate_data",
        python_callable=aggregate_data,
        dag=dag,
    )
    # # QuickSight SPICE 새로고침 시작
    # quicksight_create_ingestion = QuickSightCreateIngestionOperator(
    #     task_id="quicksight_create_ingestion",
    #     data_set_id=DATA_SET_ID,
    #     ingestion_id=INGESTION_ID,
    #     wait_for_completion=True,  # 대기 할 거임
    #     dag=dag,
    # )


bronze_layer >> silver_layer >> gold_layer
