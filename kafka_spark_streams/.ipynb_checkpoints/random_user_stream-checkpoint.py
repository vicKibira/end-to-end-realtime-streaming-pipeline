import logging
import time
from kafka import KafkaProducer
import json
import requests
import uuid


# Function to download data from the API
def download_data(url):
    req = requests.get(url)
    req = req.json()  # Raise an exception for 4xx or 5xx status codes
    if req:
        req = req['results'][0]
    return req   


# Function to format the data
def format_data(req):
    data = {}
    location = req['location']
    data['first_name'] = req['name']['first']
    data['last_name'] = req['name']['last']
    data['gender'] = req['gender']
    data['addreqs'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = req['email']
    data['username'] = req['login']['username']
    data['dob'] = req['dob']['date']
    data['registered_date'] = req['registered']['date']
    data['phone'] = req['phone']
    data['picture'] = req['picture']['medium']

    return data


# Function to stream the data
def stream_data(data):
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    curr_time = time.time()
    while True:
        if time.time() > curr_time + 60:
            break
        try:
            producer.send('users_created', json.dumps(data).encode('utf-8'))
        except Exception as e:
            print(f"an error occured: {e.__str__()}")
            continue


if __name__ == "__main__":
    url = 'https://randomuser.me/api/'
    user_data = download_data(url)
    if user_data:
        formatted_data = format_data(user_data)
        if formatted_data:
            stream_data(formatted_data)
