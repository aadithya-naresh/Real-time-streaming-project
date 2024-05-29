import json
import threading
import websocket
from kafka import KafkaProducer
import datetime
import requests
import time
import os
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

FINNHUB_API_TOKEN = os.getenv('FINNHUB_API_TOKEN')
POLYGON_API_KEY = os.getenv('POLYGON_API_KEY')
                                 
# Symbols to fetch data for
symbols = ["AAPL", "AMZN"]

# Kafka setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Function to handle incoming messages from Finnhub WebSocket
def on_message(ws, message):
    print('In On message')
    data = json.loads(message)

    # Check if message contains trade data
    for data in data['data']:
        if 's' in data and 'p' in data and 'v' in data and 't' in data:
            record = {
                'symbol': data['s'],
                'timestamp': datetime.datetime.fromtimestamp(data['t'] / 1000).strftime('%Y-%m-%d %H:%M:%S'),
                'price': data['p'],
                'volume': data['v']
            }
            print(record, "\n")
            producer.send('stock-data', value=record)

# Function to handle WebSocket errors
def on_error(ws, error):
    print('Error', error)

# Function to handle WebSocket closure
def on_close():
    print("### closed ###")

# Function to handle WebSocket connection opening
def on_open(ws):
    print("On Open")
    for symbol in symbols:
        ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))
    print("On Open done")

# Function to initiate the WebSocket connection and stream data
def stream_data():
    # websocket.enableTrace(True)
    ws = websocket.WebSocketApp(
        f"wss://ws.finnhub.io?token={FINNHUB_API_TOKEN}",
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.on_open = on_open
    ws.run_forever()

def fetch_intraday_data(symbol):
    url = f'https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/2023-05-22/2024-05-22?unadjusted=true&apiKey={POLYGON_API_KEY}'
    response = requests.get(url)
    data = response.json()

    if 'error' in data:
        print(f"Error fetching data for {symbol}: {data['error']}")
    else:
        count = 0
        for result in data['results']:
            record = {
                'symbol': symbol,
                'timestamp': datetime.datetime.fromtimestamp(result['t'] / 1000).strftime('%Y-%m-%d %H:%M:%S'),  # Unix timestamp
                'price': result['c'],      # Closing price
                'volume': result['v']      # Volume
            }
            count = count + 1
            print(count)
            producer.send('stock-data', value=record)

# Main function to start the data streaming
if __name__ == '__main__':
    for symbol in symbols:
        print(symbol)
        fetch_intraday_data(symbol)
    stream_data()
