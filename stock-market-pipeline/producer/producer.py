import requests
from kafka import KafkaProducer
import json
import time

API_KEY = 'your_alpha_vantage_api_key'
symbol = 'AAPL'
url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval=1min&apikey={API_KEY}'

producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_stock_data():
    response = requests.get(url)
    data = response.json()
    time_series = data.get('Time Series (1min)', {})
    
    for timestamp, price_info in time_series.items():
        message = {
            'symbol': symbol,
            'timestamp': timestamp,
            'open': price_info['1. open'],
            'high': price_info['2. high'],
            'low': price_info['3. low'],
            'close': price_info['4. close'],
            'volume': price_info['5. volume']
        }
        producer.send('stock-market', value=message)
        time.sleep(1)

if __name__ == "__main__":
    while True:
        fetch_stock_data()
        time.sleep(60)  # Fetch new data every minute
