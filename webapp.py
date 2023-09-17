import os
import time
import boto3
import pandas as pd
import json

from dotenv import load_dotenv

load_dotenv('.env.docker.local')

lambda_client = boto3.client('lambda')

response = lambda_client.invoke(
    FunctionName='test-lambda',
    InvocationType='RequestResponse',
    LogType='Tail',
    Payload=json.dumps({"query": "select count(*) from 's3://serverless-data/yellow_tripdata_2023-04.parquet'"}).encode("utf-8")
)

res = json.loads(response['Payload'].read().decode("utf-8"))

print(res)