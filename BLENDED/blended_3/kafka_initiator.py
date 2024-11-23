 
import json
import yaml
import logging
from kafka import KafkaConsumer, KafkaProducer
from typing import Dict, Any


class KafkaInitiator:
    """
    Kafka consumer that reads from raw data topic and produces filtered streams
    """
    def __init__(self, config_path: str):
        """Initialize the Kafka consumer with configuration"""
        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)
        
        self._setup_logging()
        self._setup_kafka_consumer()
        self._setup_kafka_producer()
        
    def _setup_logging(self):
        """Configure logging"""
        # Configure basic logging settings
        logging.basicConfig(
            level=self.config['logging']['level'],
            format=self.config['logging']['format'],
            filename=self.config['logging']['file']
        )

        # Create the logger instance
        self.logger = logging.getLogger('KafkaInitiator')

        # Create a StreamHandler to output logs to the console
        console_handler = logging.StreamHandler()
        console_handler.setLevel(self.config['logging']['level'])
        console_handler.setFormatter(logging.Formatter(self.config['logging']['format']))

        # Add the console handler to the logger
        self.logger.addHandler(console_handler)
        
        
    def _setup_kafka_consumer(self):
        """Initialize Kafka consumer"""
        try:
            self.consumer = KafkaConsumer(
                bootstrap_servers=self.config['kafka']['bootstrap_servers'],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda v: json.loads(v.decode('utf-8')),
                **self.config['kafka']['consumer_config']
                )
            self.logger.info("Kafka consumer initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize Kafka consumer: {str(e)}")
            raise
        
        
    def _setup_kafka_producer(self):
        """Initialize Kafka producer"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.config['kafka']['bootstrap_servers'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda v: json.dumps(v).encode('utf-8')
                **self.config['kafka']['producer_config']
            )
            self.logger.info("Kafka producer initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize Kafka producer: {str(e)}")
            raise
