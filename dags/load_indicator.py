import requests
import pandas as pd
from clickhouse_driver import Client
import json

CH_HOST = 'localhost'
CH_DATABASE = 'who_stg'
CH_TABLE = 'INDICATORS'
CH_D_TABLE = 'D_INDICATOR'
CH_USER = 'admin'
CH_PASSWORD = 'admin'

PATH_INDICATORS = '/root/airflow/indicators.json'

# Truncate table before loading
def truncate_indicators(**kwargs):
    with Client(host=CH_HOST, user=CH_USER, password=CH_PASSWORD, database=CH_DATABASE) as client:
        client.execute(f'TRUNCATE TABLE {CH_TABLE}')
        client.execute(f'TRUNCATE TABLE {CH_D_TABLE}')

# Get all indicators
def fetch_indicators(**kwargs):
    url = 'https://ghoapi.azureedge.net/api/Indicator'
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        with open(PATH_INDICATORS, 'w') as f:
            json.dump(data['value'], f)
    else:
        raise Exception(f"Failed to fetch indicators from WHO API. Status code: {response.status_code}")

# Load indicators into ClickHouse
def load_indicators_to_clickhouse(**kwargs):
    with open(PATH_INDICATORS, 'r') as f:
        indicators = json.load(f)

    records = [(ind['IndicatorCode'], ind['IndicatorName']) for ind in indicators]

    with Client(host=CH_HOST, user=CH_USER, password=CH_PASSWORD, database=CH_DATABASE) as client:
        client.execute(f'INSERT INTO {CH_D_TABLE} (IndicatorCode, IndicatorName) VALUES', records)


def fetch_and_load_indicator_values(**kwargs):
    with open(PATH_INDICATORS, 'r') as f:
        indicators = json.load(f)

    with Client(host=CH_HOST, user=CH_USER, password=CH_PASSWORD, database=CH_DATABASE) as client:
        for indicator in indicators:
            if '_ARCHIVED' in indicator:
                continue
                
            code = indicator['IndicatorCode']
            url = f'https://ghoapi.azureedge.net/api/{code}'
            response = requests.get(url)

            if response.status_code == 200:
                data = response.json()
                if data['value']:
                    df = pd.DataFrame(data['value'])
                    df = df.fillna('')
                    
                    df = df.astype(str)

                    data = [tuple(row) for row in df.to_records(index=False)]
                    client.execute(f'INSERT INTO {CH_TABLE} VALUES', data)
                else:
                    print(f"No data found for indicator {code}")
 
            else:
                raise Exception(f"Failed to fetch data for indicator {code} from WHO API. Status code: {response.status_code}")
