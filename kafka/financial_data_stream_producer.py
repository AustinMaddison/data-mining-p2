import json
import os
import sys
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient
from alpaca_trade_api.common import URL
from alpaca_trade_api.stream import Stream

INPUT_FILE = os.path.join(os.path.dirname(__file__), 'companies.json')
TOPIC_OUT = "financial_data_streamed"

if len(sys.argv) > 1:
    INPUT_FILE = sys.argv[1]

load_dotenv()
admin = KafkaAdminClient(
    bootstrap_servers='localhost:29092',
)

producer = KafkaProducer(bootstrap_servers='localhost:29092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

code_to_company = {}

async def trade_callback(t):
    t._raw['company'] = code_to_company[t.symbol]['company']
    t._raw['sector'] = code_to_company[t.symbol]['sector']

    producer.send(TOPIC_OUT, t._raw)

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

    for company in companies:
        code = company["symbol"]
        name = company["company"]
        sector = company["sector"]

        code_to_company[code]  = {
            "company" : name,
            "sector" : sector
        }

        stream.subscribe_trades(trade_callback, code)
        print(json.dumps(company, indent=4))


if __name__ == "__main__":
    subscibe_companies()
    stream.run()
    producer.flush()

