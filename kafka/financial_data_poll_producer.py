import json
import asyncio
import os
import sys
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient
import yfinance as yf

# Arguments
INPUT_FILE = os.path.join(os.path.dirname(__file__), 'companies_extended.json')
POLL_RATE = 32

TOPIC_OUT = "financial_data_polled"

if len(sys.argv) > 1:
    INPUT_FILE = sys.argv[1]
if len(sys.argv) > 2:
    POLL_RATE = int(sys.argv[2])

load_dotenv()

# Kafka setup
admin = KafkaAdminClient(
    bootstrap_servers='localhost:29092',
)

producer = KafkaProducer(
    bootstrap_servers='localhost:29092', 
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def load_companies():
    """Load company symbols from the input JSON file."""
    with open(INPUT_FILE) as file:
        companies = json.load(file)

    print(f'\nSUBSCRIBED FINANCIAL COMPANIES')
    print(f'------------------------------')

    for data in companies:
        print(json.dumps(data, indent=4))

    return companies

def fetch_stock_data(code):
    """Fetch stock data using yfinance."""
    try:
        ticker = yf.Ticker(code)
        data = ticker.history(period="1d")
        if not data.empty:
            last_quote = data.iloc[-1]
            return {
                "symbol": code,
                "date": last_quote.name.strftime("%Y-%m-%d %H:%M:%S"),
                "open": last_quote["Open"],
                "high": last_quote["High"],
                "low": last_quote["Low"],
                "close": last_quote["Close"],
                "volume": last_quote["Volume"],
            }
        else:
            print(f"No data available for {code}.")
            return None
    except Exception as e:
        print(f"Error fetching data for {code}: {e}")
        return None

async def stream_data(companies):
    """Stream stock data at the specified poll rate."""
    poll_idx = 0

    while True:

        for company in companies:

            code = company["symbol"]
            name = company["company"]
            sector = company["sector"]

            data = fetch_stock_data(code)

            if data:
                data["company"] = name
                data["sector"] = sector

                print(data)
                producer.send(TOPIC_OUT, data)
                producer.flush()

        poll_idx += 1
        print(f"{poll_idx} POLL DONE!\n")
        await asyncio.sleep(POLL_RATE)

if __name__ == "__main__":
    companies = load_companies()
    asyncio.run(stream_data(companies))