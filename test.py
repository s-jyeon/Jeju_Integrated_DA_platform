import json

import boto3

client = boto3.client(
    service_name="secretsmanager",
    region_name="ap-northeast-2",
)
get_secret_value_response = client.get_secret_value(
    SecretId="ip-jeju-jeju-data-hub-key"
)
secret = json.loads(get_secret_value_response["SecretString"])["KEY"]
print(secret)
