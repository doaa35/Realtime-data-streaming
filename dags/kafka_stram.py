import uuid
from datetime import datetime


# import dag object

# DAG object is a Python script that defines a workflow as a DAG structure. 
# It encapsulates the entire workflow logic

from airflow import DAG

# import python operator
from airflow.operators.python import PythonOperator


#default arge or cong --- the same dict for our airflow projects
default_args= {
    'owner': 'doaa_alshorbagy',
    'start_date': datetime(2024, 8, 11) # str(datetime.now().date())

}

# get data from api
def get_data():
    import requests

    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]

    return res

# format the data coming from api
def format_data(res):
    data = {}
    location = res['location']
    data['id'] =str(uuid.uuid4())
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data


# Function to stream data to Kafka
def stream_data():
    import json

    #A class from the kafka library used to send messages to Kafka topics
    from kafka import KafkaProducer

    import time

   #logging Provides a flexible framework for logging messages, useful for debugging and monitoring.
    import logging


    # Create Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=['broker:29092'],  # Adjust to our Kafka broker configuration
        max_block_ms=5000
    )

    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60:  # Run for 1 minute
            break
        try:
            # Fetch and format data
            res = get_data()
            formatted_data = format_data(res)

            # Send data to Kafka topic
            producer.send('_created', json.dumps(formatted_data).encode('utf-8'))


	    # Flush producer to ensure data is sent
            producer.flush()

            # Sleep to avoid busy-waiting
            time.sleep(5)  # Sleep for 5 seconds before the next iteration

        except Exception as e:
            logging.error(f'Error: {e}')
            continue

# Define the DAG
with DAG(
    dag_id='automation',
    default_args=default_args, 
    schedule='@daily',
    catchup=False

) as dag:
    task= PythonOperator(
        task_id='data_from_api',
	python_callable=stream_data
    )
