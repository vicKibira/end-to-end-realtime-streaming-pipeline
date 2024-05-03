from typing import  Dict, List
from json import loads
from kafka import KafkaConsumer
import json



def consume_from_kafka(topic_name='users_created',bootstrap_servers=['localhost:9092'], group_id='my_consumer_group'):

    try:
        consumer = KafkaConsumer(topic_name,group_id=group_id,bootstrap_servers=bootstrap_servers,value_deserializer=lambda x: json.loads(x.decode('utf-8')))

        print(f"kafka consumer connecte successfully")
        #continuosly listen and process messages

        for message in consumer:
            data = message.value
            print("Received message")
            print(json.dumps(data, indent=2))

    except Exception as e:
        print(f"an error occured in kafka consumer: {e}")
    finally:
        consumer.close()


if __name__ == "__main__":
      consume_from_kafka()
        
      


