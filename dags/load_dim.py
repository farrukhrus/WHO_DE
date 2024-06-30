import requests
import time
import pandas as pd
from clickhouse_driver import Client
import json
from kafka import KafkaProducer, KafkaConsumer, TopicPartition

CH_HOST = 'localhost'
CH_DATABASE = 'who_stg'
CH_D_TABLE = 'D_DIMENSION'
CH_TABLE = 'DIMENSIONS'
CH_USER = 'admin'
CH_PASSWORD = 'admin'

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'dimension_values'

PATH_DIM = '/root/airflow/dimensions.json'


# Truncate table before loading
def truncate_dimensions(**kwargs):
    with Client(host=CH_HOST, user=CH_USER, password=CH_PASSWORD,
                database=CH_DATABASE) as client:
        client.execute(f'TRUNCATE TABLE {CH_TABLE}')
        client.execute(f'TRUNCATE TABLE {CH_D_TABLE}')


# Get all dimensions
def fetch_dimensions(**kwargs):
    url = 'https://ghoapi.azureedge.net/api/Dimension'
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        with open(PATH_DIM, 'w') as f:
            json.dump(data['value'], f)
    else:
        raise Exception(f"Failed to fetch dimensions. Status code: {response.status_code}")


# Load dimensions into ClickHouse
def load_dimensions_to_clickhouse(**kwargs):
    with open(PATH_DIM, 'r') as f:
        dimensions = json.load(f)

    records = [(dim['Code'], dim['Title']) for dim in dimensions]

    with Client(host=CH_HOST, user=CH_USER, password=CH_PASSWORD,
                database=CH_DATABASE) as client:
        client.execute(f'INSERT INTO {CH_D_TABLE} (Code, Title) VALUES', records)


# Get dimension values and produce to Kafka
def fetch_and_produce_dimension_values(**kwargs):
    with open(PATH_DIM, 'r') as f:
        dimensions = json.load(f)

    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    for dimension in dimensions:
        code = dimension['Code']
        url = f'https://ghoapi.azureedge.net/api/DIMENSION/{code}/DimensionValues'
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            if data['value']:
                for value in data['value']:
                    record = {
                        'DimCode': code,
                        'Code': value.get('Code', ''),
                        'Title': value.get('Title', ''),
                        'ParentDimension': value.get('ParentDimension', ''),
                        'Dimension': value.get('Dimension', ''),
                        'ParentCode': value.get('ParentCode', ''),
                        'ParentTitle': value.get('ParentTitle', '')
                    }

                    key = f"{value.get('Code', code)}_{value.get('Title', '')}".encode('utf-8')
                    producer.send(KAFKA_TOPIC, value=record, key=key)
            else:
                print(f"No data found for dimension {code}")
        else:
            raise Exception(f"Failed to fetch data for dimension {code}. Status code: {response.status_code}")

    producer.flush()
    producer.close()


# Consume data from Kafka and load into ClickHouse
def consume_and_load_dimension_values(**kwargs):
    processed_keys = set()

    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        group_id='dimension-values-loader'
    )

    partition = TopicPartition(KAFKA_TOPIC, 0)
    consumer.assign([partition])

    end_offsets = consumer.end_offsets([partition])
    end_offset = end_offsets[partition]

    with Client(host=CH_HOST, user=CH_USER, password=CH_PASSWORD,
                database=CH_DATABASE) as client:
        for message in consumer:
            key = message.key.decode('utf-8')
            if key in processed_keys:
                continue

            processed_keys.add(key)

            value = message.value
            df = pd.DataFrame([value])
            df = df.fillna('')
            df = df.astype(str)

            records = [tuple(row) for row in df.to_records(index=False)]
            client.execute(f'INSERT INTO {CH_TABLE} VALUES', records)
            consumer.commit()

            if message.offset + 1 >= end_offset:
                print("No messages left. Finishing.")
                break
 
    consumer.close()
