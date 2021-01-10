# KAFKA PRODUCER

from time import sleep
from json import dumps
from kafka import KafkaProducer
import requests

# Create Producer Via Kafka
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

# Produce Coordinates for ISS
streamin = True

while streamin is True:
    coord = requests.get('http://api.open-notify.org/iss-now.json').json()
    data = {
        'lat': coord['iss_position']['latitude'],
        'long': coord['iss_position']['longitude'],
        'tm': coord['timestamp']
            }
    producer.send('NewTopic', value=data)
    sleep(5)
    
#%%