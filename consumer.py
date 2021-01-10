# KAFKA CONSUMER

from kafka import KafkaConsumer
from json import loads

# Consume Most Recent
consumer = KafkaConsumer(
    'NewTopic',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=False,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

for message in consumer:
    print(message.value)
    
#%%