import json
import sys
import os
import duckdb

# global DB connection, to be reused across invocations
conn = None

# max number of rows to return
rows_limit = 10

def get_conn_with_s3():
    s3_region = os.environ['AWS_REGION']
    s3_key = os.environ['AWS_KEY_ID']
    s3_secret = os.environ['AWS_SECRET_KEY']
    s3_session = os.environ['AWS_SESSION_TOKEN']

    conn = duckdb.connect(database=':memory:')

    # conn.execute(f"""
    #     LOAD httpfs;
    #     SET s3_region='{s3_region}';
    #     SET s3_session_token='{s3_session}';
    # """)

    conn.execute(f"""
        LOAD httpfs;
        SET s3_region='{s3_region}';
        SET s3_access_key_id = '{s3_key}';
        SET s3_secret_access_key = '{s3_secret}';
        SET s3_session_token='{s3_session}';
    """)

    # SET s3_access_key_id = '{os.environ['AWS_KEY_ID']}';
    # SET s3_secret_access_key = '{os.environ['AWS_SECRET_KEY']}'

    return conn

def lambdaHandler(event, context):
    print(f"Hello lambda - DuckDb coming! Version: {sys.version}")
    print(f"Payload: {event}\nQuery: {event['query']}")

    s3_bucket = 'serverless-data'

    global conn

    if conn is None:
        conn = get_conn_with_s3()
        print(f"New connection created - assuming cold run.")
    else:
        print(f"Connection already exists - assuming warm run.")

    # _df = conn.execute(f"select count(*) from 's3://sd-demo-bucket/data/compensated_nyc_trips.parquet'").df()
    # _df = conn.execute(f"select count(*) from 's3://{s3_bucket}/*.parquet'").df()
    _df = conn.execute(event['query']).df()
    print(f"Dataframe got from the data.")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Hello from Lambda - DuckDb coming!",
            "query": event['query'],
            "data": _df.to_json()
        }),
    }

if __name__ == "__main__":
    if os.getenv('AWS_REGION') is None:
        os.environ['AWS_REGION'] = 'eu-west-3'

        f = open('assume-role-output.json')
        data = json.load(f)

        os.environ['AWS_KEY_ID'] = data['Credentials']['AccessKeyId']
        os.environ['AWS_SECRET_KEY'] = data['Credentials']['SecretAccessKey']
        os.environ['AWS_SESSION_TOKEN'] = data['Credentials']['SessionToken']

    res = lambdaHandler({"query": "select count(*) from 's3://serverless-data/*.parquet'"}, None)

    print(res)