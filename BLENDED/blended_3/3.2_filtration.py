import time

from kafka import KafkaConsumer
from configs import kafka_config
import json
from kafka import KafkaProducer
from configs import kafka_config
import json
import uuid
import time
import random

# Створення Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# sensor_id = random.randint(1, 100)

# for i in range(99999):
#     try:
#         data = {
#             "sensor_id": sensor_id,
#             "timestamp": time.time(),
#             "temperature": random.randint(25, 45),
#             "humidity": random.randint(15, 85)
#         }
#         producer.send(topic_name, key=str(uuid.uuid4()), value=data)
#         producer.flush()
#         print(f"Message {i} sent to topic '{topic_name}' successfully.")
#         time.sleep(2)
#     except Exception as e:
#         print(f"An error occurred: {e}")

# producer.close()

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

topic_name = "currency_raw_data"

consumer.subscribe([topic_name])

print(f"Subscribed to topic '{topic_name}'")

try:
    i = 0
    for message in consumer:

        data = message.value
        i +=1
        print(f"{i} - Received {data['symbol']}")
        
        if data['symbol'] == None:
            continue

        producer.send(f'topic_{data["symbol"]}', key=str(uuid.uuid4()), value=data)
        producer.flush()

        time.sleep(0.05)
        
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()
