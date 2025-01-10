import io

import boto3
import pandas as pd

s3_client = boto3.client("s3", region_name="ap-northeast-2")

# S3 객체 다운로드
response = s3_client.get_object(
    Bucket="ip-jeju-raws",
    Key="jeju_car_sharing_company_locations/jeju_car_sharing_company_locations.csv",
)
print(f"Content Length: {response['ContentLength']} bytes")
# print(response["Body"].read().decode("utf-8"))
csv_content = response["Body"].read().decode("utf-8")  # CSV 데이터를 문자열로 읽기

# CSV 데이터를 데이터프레임으로 변환
df = pd.read_csv(io.StringIO(csv_content))
print(f"Fetched {len(df)} rows from S3")
