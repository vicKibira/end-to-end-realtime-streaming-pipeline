from kafka import KafkaProducer
import logging
import json
import csv
import requests
import time
from confluent_kafka import Producer

def download_data():
    url = 'https://randomuser.me/api/'
    req = requests.get(url)
    req = req.json()
    return req

def format_data(req):
    data = {}
    data['first_name'] = req['results'][0]['name']['first']
    data['last_name'] = req['results'][0]['name']['last']
    data['gender'] = req['results'][0]['gender']
    return data

def stream_data():
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    current_time = time.time()

    while True:
        if time.time() > current_time + 60:
            break
        try:
            req = download_data()
            data = format_data(req)
            producer.send('users_created', json.dumps(data).encode('utf-8'))
            time.sleep(1)  # Add a short sleep to prevent flooding the server
        except Exception as e:
            logging.error(f"an error occurred: {e}")

stream_data()
