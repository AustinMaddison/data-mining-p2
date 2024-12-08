from kafka import KafkaProducer
import json
import requests

import os
from dotenv import load_dotenv


load_dotenv()

producer = KafkaProducer(bootstrap_servers='localhost:29092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def fetch_financial_data(api_key, symbol):

    url = f""
    response = requests.get(url)
    data = response.json()
    return data

def stream_data():
    api_key = os.getenv('API_KEY')
    
    data = fetch_financial_data()
    producer.send('financial-data', data)

if __name__ == "__main__":
    stream_data()