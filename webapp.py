import os
import time
import boto3
import pandas as pd
import json
from dash import Dash, html, dcc, callback, Output, Input, State
from dash.exceptions import PreventUpdate

from dotenv import load_dotenv

load_dotenv('.env.docker.local')

ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

lambda_client = boto3.client('lambda',
                             aws_access_key_id=ACCESS_KEY,
                             aws_secret_access_key=SECRET_KEY,
                             )


app = Dash(__name__)
app.layout = html.Div([
    html.H1(children='Serverless DuckDb lambda', style={'textAlign':'center'}),
    html.Label('Minimal trip cost:'),
    dcc.Input(id='minimal_cost', value='0', type='text'),
    html.Button('Show hourly trips', id='submit-val', n_clicks=0),

    dcc.Graph(id='graph-content'),

    html.Hr(),
    html.Div([html.Pre(id='lambda-response')])
])

# dash callback to update graph on button click
@app.callback(
    Output('graph-content', 'figure'),
    Output('lambda-response', 'children'),
    Input('submit-val', 'n_clicks'),
    State('minimal_cost', 'value')
)
def update_graph(n_clicks, minimal_cost):

    # qry = "select count(*) from 's3://serverless-data/yellow_tripdata_2023-04.parquet'"

    qry = '''
    SELECT
        hour(tpep_pickup_datetime) AS hour_num, 
        COUNT(*) AS trips_count
    FROM 's3://serverless-data/*.parquet'
    WHERE total_amount >= {minimal_cost}
    GROUP BY 1
    ORDER BY 1
    '''

    qry = qry.replace('{minimal_cost}', minimal_cost)

    # qry = '''
    # SELECT * FROM read_parquet(['s3://serverless-data/yellow_tripdata_2023-04.parquet'])
    # LIMIT 1
    # '''

    response = lambda_client.invoke(
        FunctionName='test-lambda',
        InvocationType='RequestResponse',
        LogType='Tail',
        Payload=json.dumps({"query": qry}).encode("utf-8")
    )

    resp_payload = response['Payload'].read().decode("utf-8")

    # save response to file
    with open('lambda_response.json', 'w') as f:
        f.write(resp_payload)

    # read response from file
    with open('lambda_response.json', 'r') as f:
        resp_payload = f.read()

    res = json.loads(resp_payload)

    if 'errorMessage' in res:
        print(f"Error: {res['errorMessage']}")
        return {}, res['errorMessage']
    else:
        body_json = json.loads(res['body'])

        df = pd.read_json(body_json['data'])

        return [
            {
                'data': [
                    {'x': df['hour_num'], 'y': df['trips_count'], 'type': 'bar', 'name': 'SF'},
                ],
                'layout': {
                    'title': 'Trips per hour'
                }
            },
            json.dumps(body_json, indent=4, sort_keys=True)
        ]

if __name__ == '__main__':
    app.run(debug=True)