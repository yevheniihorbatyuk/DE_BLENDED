import time

from kafka import KafkaConsumer
from configs import kafka_config
import json

consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my_consumer_group_1'
)

topic_name = "building_sensors_greenmoon"

consumer.subscribe([topic_name])

print(f"Subscribed to topic '{topic_name}'")

try:
    for message in consumer:

        data = message.value
        print(f"Received {data}")

        # if data["temperature"] > 40 or data["humidity"] < 20 or data["humidity"] > 80:
        #     print(f"Data to send {data}")

        # time.sleep(1)
        
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()
