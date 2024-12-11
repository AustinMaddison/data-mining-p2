import json
import requests
import os
import asyncio
import sys
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient

# arguments
INPUT_FILE = os.path.join(os.path.dirname(__file__), 'companies.json')
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

def fetch_currency_data(code):
    url = "https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=" + code + "&apikey=" + os.getenv('ALPACA_API_KEY')
    r = requests.get(url)
    return r.json()

def load_companies():
    codes = []
    # loads company codes from json to callback function
    with open(INPUT_FILE) as file:
        companies: json = json.load(file)

    print(f'\nSUBSCRIBED FINANCIAL COMPANIES')
    print(f'------------------------------')

    for data in companies:
        codes += [data.get("symbol")]
        print(json.dumps(data, indent=4))
    
    return codes

async def stream_data(codes):
    poll_idx = 0
    
    while True:
        poll_idx += 1

        for code in codes:
            data: json = fetch_currency_data(code)

            
            if 'Information' in data:
                print("API rate limit reached. Please try again later.")
                await asyncio.sleep(POLL_RATE)
                continue

            producer.send('financial_data', data)
            producer.flush()
            print(data)
        
        print(f"{poll_idx} POLL DONE!\n")
        await asyncio.sleep(POLL_RATE)

if __name__ == "__main__":
    codes = load_companies()
    asyncio.run(stream_data(codes))




