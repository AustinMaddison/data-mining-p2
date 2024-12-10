from time import sleep

from kafka import KafkaProducer
import json
import requests

import os
from dotenv import load_dotenv
#from kafka import KafkaAdminClient
from kafka.admin import KafkaAdminClient

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
financial_data = os.getenv('STOCK_LOG')

from alpaca_trade_api.common import URL
from alpaca_trade_api.stream import Stream


ALPACA_API_KEY = "PK4P6XJV9I8CA9ZAKJ72"
ALPACA_SECRET_KEY = "GKNn9h0FCmGCDSJPZCJAUqLekbTxThAnUuLMgDtZ"



producer = KafkaProducer(bootstrap_servers='localhost:29092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def fetch_financial_data(api_key, symbol):
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval=5min&apikey={api_key}'
    response = requests.get(url)
    data = response.json()
    return data



async def trade_callback(t):
    producer.send('financial-data', t._raw)
    print('trade', t._raw)


async def quote_callback(q):
    producer.send('financial-data', q._raw)
    print('quote', q._raw)


# Initiate Class Instance
stream = Stream(ALPACA_API_KEY,
                ALPACA_SECRET_KEY,
                base_url=URL('wss://stream.data.alpaca.markets/v2/'),
                data_feed='iex')  # <- replace to 'sip' if you have PRO subscription

# subscribing to event
stream.subscribe_trades(trade_callback, 'AAPL')
stream.subscribe_quotes(quote_callback, 'IBM')

def stream_data():
    api_key = os.getenv('API_KEY')
    data = fetch_financial_data(api_key, 'aapl')
    producer.send('financial-data', data)
    print(data)
    stream.run()
    producer.flush()

if __name__ == "__main__":
    stream_data()
