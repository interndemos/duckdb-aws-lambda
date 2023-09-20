import json
import sys
import os
import urllib.parse
import time

import duckdb


# global DB connection, to be reused across invocations
conn = None

# max number of rows to return
rows_limit = 10

def get_conn_with_s3():
    s3_region = os.environ['AWS_REGION']
    s3_key = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret = os.environ['AWS_SECRET_ACCESS_KEY']
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
    print(f"Hello lambda - DuckDb coming! Version: {sys.version}; Man ID: 3")

    is_warm = False
    conn_time = 0

    s3_bucket = 'serverless-data'

    if 'query' in event:
        qry = urllib.parse.unquote(event['query'])
        print(f"Payload: {event}\nQuery: {qry}")
    else:
        qry = f"select count(*) from 's3://{s3_bucket}/yellow_tripdata_2023-04.parquet'"
        print(f"Payload: {event}\nNo query passed - using default: {qry}")

    global conn

    if conn is None:
        start = time.time()
        conn = get_conn_with_s3()
        end = time.time()
        conn_time = end - start
        print(f"New connection created - assuming cold run. Time to create connection: {conn_time}")
    else:
        print(f"Connection already exists - assuming warm run.")
        is_warm = True

    # measure time
    start = time.time()
    # _df = conn.execute(f"select count(*) from 's3://sd-demo-bucket/data/compensated_nyc_trips.parquet'").df()
    _df = conn.execute(qry).df()
    end = time.time()

    query_time = end - start

    print(f"Dataframe got from the data. Time to execute query: {query_time}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "is_warm": is_warm,
            "connection_duration": conn_time,
            "query duration": query_time,
            "message": "Hello from Lambda - DuckDb coming!",
            "query": qry,
            "data": _df.to_json(orient='records')
        }),
    }

# if __name__ == "__main__":
#     print("Run in standalone mode - main started.")
#
#     if os.getenv('AWS_REGION') is None:
#         os.environ['AWS_REGION'] = 'eu-west-3'
#
#         f = open('assume-role-output.json')
#         data = json.load(f)
#
#         os.environ['AWS_ACCESS_KEY_ID'] = data['Credentials']['AccessKeyId']
#         os.environ['AWS_SECRET_ACCESS_KEY'] = data['Credentials']['SecretAccessKey']
#         os.environ['AWS_SESSION_TOKEN'] = data['Credentials']['SessionToken']
#
#     qry = "select filename, count(*) from read_parquet('s3://serverless-data/*.parquet', filename=true) group by 1"
#
#     qry = '''
#     SELECT
#         hour(tpep_pickup_datetime) AS hour_num,
#         SUM(passenger_count) AS trips_count
#     FROM 's3://serverless-data/yellow_tripdata_2023-05.parquet'
#     WHERE total_amount >= 5
#     GROUP BY 1
#     ORDER BY 1
#     '''
#
#     # qry = "select * from read_parquet('s3://serverless-data/*.parquet', filename=true) LIMIT 1"
#     # qry = "describe select * from read_parquet('s3://serverless-data/*.parquet')"
#
#     res = lambdaHandler({"query": qry}, None)
#     # res = lambdaHandler({}, None)
#
#     print(res)