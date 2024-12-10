import pprint
from time import sleep

import pandas as pd
from alpaca.data import Trade
from alpaca_trade_api.entity import ISO8601YMD, _NanoTimestamped
from kafka import KafkaProducer
import json
import requests

import os
from dotenv import load_dotenv
from kafka import KafkaAdminClient
from sqlalchemy.util import symbol
import time

# from kafka.admin import KafkaAdminClient

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

producer = KafkaProducer(bootstrap_servers='localhost:29092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def fetch_financial_data(api_key, symbol):
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval=5min&apikey={api_key}'
    response = requests.get(url)
    data = response.json()
    return data

def stream_data():
    f = open('symbols.json')
    symbols = json.load(f)
    for symbol in symbols:
        api_key = os.getenv('API_KEY')
        apiKey = '6E87VKYTFU8HOT6J'
        apiKey2= 'X4FDF7HQHOIV0YF9'
        data = fetch_financial_data(apiKey2, symbol["symbol"])
        producer.send('financial-data', data)
        producer.flush()


from alpaca_trade_api.common import URL
from alpaca_trade_api.stream import Stream


async def trade_callback(t):
    trade_data = t if isinstance(t, dict) else t.__dict__
    print(trade_data)
    producer.send('financial-data', trade_data)
    producer.flush()


# async def quote_callback(q):
#     return q



# Initiate Class Instance
stream = Stream("PKHL0MBLB5UWPWU4GJTP",
                "Zms9rxshMqgtZeX6rlUHuusj7PGrtGlPhNR20lbP",
                base_url=URL('wss://stream.data.alpaca.markets/v2/'),
                data_feed='iex')  # <- replace to 'sip' if you have PRO subscription

# subscribing to event
stream.subscribe_trades(trade_callback, 'AAPL')
#stream.subscribe_quotes(quote_callback, 'IBM')

if __name__ == "__main__":
    stream.run()
