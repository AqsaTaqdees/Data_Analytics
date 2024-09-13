import uuid
import requests
import json
import logging
from kafka import KafkaProducer
import time

logging.basicConfig(level=logging.INFO)

def get_data():
    res = requests.get("https://randomuser.me/api/")
    res.raise_for_status()  # raise an error if bad status code occurs
    return res.json()['results'][0]

def format_data(res):
    data = {}
    location = res['location']
    data['id'] = str(uuid.uuid4())  # converting UUID to string
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{location['street']['number']} {location['street']['name']}, {location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    return data

def stream_data():
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000)
    users_to_fetch = 10
    fetch_interval = 10  # Fetch data every 10 seconds
    max_batches = 5  # Maximum number of batches

    batch_count = 0

    while batch_count < max_batches:
        try:
            for _ in range(users_to_fetch):
                res = get_data()
                formatted_data = format_data(res)
                producer.send('users_created', json.dumps(formatted_data).encode('utf-8'))
                logging.info("Data sent to Kafka topic 'users_created'")

            batch_count += 1
            logging.info(f"Fetched data for {users_to_fetch} users (Batch {batch_count}/{max_batches})")

        except Exception as e:
            logging.error(f"An error occurred: {e}")

        time.sleep(fetch_interval)  # Wait for 10 seconds before fetching the next batch

    logging.info("Reached maximum number of batches. Stopping stream.")
stream_data()
