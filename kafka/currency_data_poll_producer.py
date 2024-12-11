import json
import requests
import os
import sys
import asyncio
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient

# arguments
INPUT_FILE = os.path.join(os.path.dirname(__file__), 'currencies.json')
POLL_RATE = 32

if len(sys.argv) > 1:
    INPUT_FILE = sys.argv[1]
if len(sys.argv) > 2:
    POLL_RATE = int(sys.argv[2])


load_dotenv()

admin = KafkaAdminClient(
    bootstrap_servers='localhost:29092',
)

producer = KafkaProducer(bootstrap_servers='localhost:29092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def fetch_currency_data(codes):
    """Fetch currency exchange data using freecurrency."""
    currencies = "currencies=" + "%2C".join(codes)
    # base_currency = "base_currency=USD" # get curencies relative to USD
    url = "https://api.freecurrencyapi.com/v1/latest?apikey="+ os.getenv('FREE_CURR_KEY') + "&" + currencies
    
    r = requests.get(url)
    return r.json()

    
def load_currencies():
    # read currency code from json and subscibes them for callbck func
    codes = []
    
    with open(INPUT_FILE) as file:
        currency_data: json = json.load(file)

    print(f'\nSUBSCIBED CURRENCIES')
    print(f'---------------------')
    
    for data in currency_data.get('countries'):
        codes += [data.get("currencyCode")]
        print(json.dumps(data, indent=4))
    
    return codes

async def stream_data(codes):
    """Stream stock data at the specified poll rate."""
    poll_idx = 0

    while True:
        poll_idx += 1

        data = fetch_currency_data(codes)
        producer.send('currency_data', data)
        print(data)
        producer.flush()

        print(f"{poll_idx} POLL DONE!\n")
        await asyncio.sleep(POLL_RATE) 

if __name__ == "__main__":
    codes = load_currencies()
    asyncio.run(stream_data(codes))

