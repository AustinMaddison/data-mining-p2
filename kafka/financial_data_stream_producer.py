import json
import os
import sys
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient
from alpaca_trade_api.common import URL
from alpaca_trade_api.stream import Stream

INPUT_FILE = os.path.join(os.path.dirname(__file__), 'companies.json')

if len(sys.argv) > 1:
    INPUT_FILE = sys.argv[1]

load_dotenv()
admin = KafkaAdminClient(
    bootstrap_servers='localhost:29092',
)

producer = KafkaProducer(bootstrap_servers='localhost:29092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

async def trade_callback(t):
    producer.send('financial-data', t._raw)

stream = Stream(os.getenv('ALPACA_API_KEY'),
                os.getenv('ALPACA_SECRET_KEY'),
                base_url=URL('wss://stream.data.alpaca.markets/v2/'),
                data_feed='iex')  # <- replace to 'sip' if you have PRO subscription

def subscibe_companies():
    # subscribe company codes from json to callback function
    with open(INPUT_FILE) as file:
        companies: json = json.load(file)

    print(f'\nSUBSCRIBED FINANCIAL COMPANIES')
    print(f'------------------------------')

    for data in companies:
        code = data.get("symbol") 
        stream.subscribe_trades(trade_callback, code)
        print(json.dumps(data, indent=4))


if __name__ == "__main__":
    subscibe_companies()
    stream.run()
    producer.flush()

