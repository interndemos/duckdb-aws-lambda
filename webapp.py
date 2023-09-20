import os
import time
import boto3
import pandas as pd
import json

from dotenv import load_dotenv

load_dotenv('.env.docker.local')

ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

lambda_client = boto3.client('lambda',
                             aws_access_key_id=ACCESS_KEY,
                             aws_secret_access_key=SECRET_KEY,
                             )

# qry = "select count(*) from 's3://serverless-data/yellow_tripdata_2023-04.parquet'"

qry = '''
SELECT
  PULocationID AS location_id,
    COUNT(*) AS counts
FROM read_parquet(['s3://serverless-data/yellow_tripdata_2023-04.parquet'])
GROUP BY 1
'''

response = lambda_client.invoke(
    FunctionName='test-lambda',
    InvocationType='RequestResponse',
    LogType='Tail',
    Payload=json.dumps({"query": qry}).encode("utf-8")
)

res = json.loads(response['Payload'].read().decode("utf-8"))

print(res)