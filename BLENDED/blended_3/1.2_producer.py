
import asyncio
import websockets
import json
from kafka import KafkaProducer
import uuid
import time
from kafka_initiator import KafkaInitiator
from configs import kafka_config

initiator = KafkaInitiator('BLENDED/blended_3/config.yaml')

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

# Конфігурація
BINANCE_WS_URL =  initiator.config['binance'].get('websocket_url',  "wss://stream.binance.com:9443/ws")
CURRENCIES = ['xrpusdt']  # Вибір валют


async def binance_ws_client():
    # Формуємо запит для підписки на кілька валют
    params = {
        "method": "SUBSCRIBE",
        "params": [f"{symbol}@trade" for symbol in CURRENCIES],
        "id": 1
    }
    async with websockets.connect(BINANCE_WS_URL) as ws:
        await ws.send(json.dumps(params))
        i = 0
        topic_name = 'currency_raw_data'
        while True:
            message = await ws.recv()
            raw_data = json.loads(message)

            # Перетворення структури повідомлення
            data = {
                "event_type": raw_data.get("e"),
                "event_time": raw_data.get("E"),
                "symbol": raw_data.get("s"),
                "trade_id": raw_data.get("t"),
                "price": float(raw_data.get("p", 0)),
                "quantity": float(raw_data.get("q", 0)),
                "trade_time": raw_data.get("T"),
                "buyer_market_maker": raw_data.get("m"),
                "ignore": raw_data.get("M")
            }

            # Надсилання повідомлення до Kafka
            producer.send('currency_raw_data', key=str(uuid.uuid4()), value=data)
            producer.flush()
            print(f"Message {i} sent to topic '{topic_name}' successfully. {data}")
            i += 1

            time.sleep(1)
            
            
if __name__ == "__main__":
    asyncio.run(binance_ws_client())
