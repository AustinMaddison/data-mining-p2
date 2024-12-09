from time import sleep

from kafka import KafkaProducer
import json
import requests

import os
from dotenv import load_dotenv
from kafka.admin import KafkaAdminClient, NewTopic

# Kafka admin setup
admin = KafkaAdminClient(
    bootstrap_servers='localhost:29092',
)
topics_list = admin.list_topics()
# Admin logic

# if "financial-data" not in topics_list:
#   topics = [NewTopic(name="financial-data", num_partitions=2, replication_factor=1)]
#   admin.create_topics(new_topics=topics, validate_only=False)
#
# else:
#        print(topics_list)


load_dotenv()

producer = KafkaProducer(bootstrap_servers='localhost:29092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def fetch_financial_data(api_key, symbol):

    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval=5min&apikey={api_key}'
    response = requests.get(url)
    data = response.json()
    #print(data)
    return data

def stream_data():
    api_key = os.getenv('API_KEY')
    data = fetch_financial_data(api_key, 'aapl')
    #print(data)
    producer.send('financial-data', data)
    producer.flush()

if __name__ == "__main__":
    stream_data()
