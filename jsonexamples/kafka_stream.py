import json
import requests
import logging
from datetime import datetime
from kafka import KafkaProducer
import time


def pull_data(url, params):
    req = requests.get(url, params=params)
    req = req.json()
    return req
    
def format_data(req):
    data = {}

    data['holiday_name'] = req['holidays'][0]['name']
    data['date'] = req['holidays'][0]['date']
    data['observed_date'] = req['holidays'][0]['observed']
    data['country'] = req['holidays'][0]['country']

    return data

def stream_data(data):
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60:
            break
        try:
            producer.send('holiday_data_topic', json.dumps(data).encode('utf-8'))
            time.sleep(0.4)  # Sleep for a second before sending the next message
        except Exception as e:
            logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    url = "https://holidayapi.com/v1/holidays"
    params = {
        'key': '7f481c63-f476-4fc0-827f-48a462d52cf1',
        'year': 2023,
        'country': 'BR'
    }

    # Pull data
    holiday_data = pull_data(url, params)

    # Format data
    formatted_data = format_data(holiday_data)

    # Stream data
    stream_data(formatted_data)
