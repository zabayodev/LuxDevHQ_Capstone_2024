from kafka import KafkaConsumer
import json
from collections import deque

consumer = KafkaConsumer(
    'stock-market',
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.loads(v.decode('utf-8'))
)

window_size = 5
price_window = deque(maxlen=window_size)

def calculate_moving_average(stock_data):
    price_window.append(float(stock_data['close']))
    if len(price_window) == window_size:
        moving_avg = sum(price_window) / window_size
        print(f"Moving Average for {stock_data['symbol']} at {stock_data['timestamp']}: {moving_avg}")

def process_stock_data():
    for message in consumer:
        stock_data = message.value
        calculate_moving_average(stock_data)

if __name__ == "__main__":
    process_stock_data()
