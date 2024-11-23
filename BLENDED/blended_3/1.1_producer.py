
import uuid
import time
import random
from configs import kafka_config
from kafka import KafkaProducer
import json

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


topic_name = "building_sensors_greenmoon"

sensor_id = random.randint(1, 5)

for i in range(99999):
    try:
        data = {
            "sensor_id": sensor_id,
            "timestamp": time.time(),
            "temperature": random.randint(25, 45),
            "humidity": random.randint(15, 85)
        }
        
        producer.send(topic_name, key=str(uuid.uuid4()), value=data)
        producer.flush()
        print(f"Message {i} sent to topic '{topic_name}' successfully.")
        time.sleep(2)
    except Exception as e:
        print(f"An error occurred: {e}")

producer.close()


