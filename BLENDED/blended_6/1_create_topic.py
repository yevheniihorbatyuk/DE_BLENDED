from dataclasses import dataclass
import os
import logging
from typing import Dict, List, Optional
from enum import Enum
from kafka.admin import KafkaAdminClient
from kafka.errors import KafkaError
from kafka.admin.new_topic import NewTopic
from colorama import Fore, Style, init

# Initialize colorama
init(autoreset=True)

# Configure logging with a custom formatter
class ColoredFormatter(logging.Formatter):
    COLORS = {
        'WARNING': Fore.YELLOW,
        'ERROR': Fore.RED,
        'INFO': Fore.GREEN,
        'DEBUG': Fore.BLUE
    }

    def format(self, record):
        if record.levelname in self.COLORS:
            record.msg = f"{self.COLORS[record.levelname]}{record.msg}{Style.RESET_ALL}"
        return super().format(record)

# Set up logging
logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
handler.setFormatter(ColoredFormatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)
logger.setLevel(logging.INFO)

class SecurityProtocol(Enum):
    PLAINTEXT = "PLAINTEXT"
    SASL_PLAINTEXT = "SASL_PLAINTEXT"
    SSL = "SSL"
    SASL_SSL = "SASL_SSL"

@dataclass
class KafkaConfig:
    bootstrap_servers: str
    username: str
    password: str
    security_protocol: SecurityProtocol
    sasl_mechanism: str
    topic_prefix: str
    num_partitions: int = 2
    replication_factor: int = 1

    @classmethod
    def from_env(cls) -> 'KafkaConfig':
        """Create configuration from environment variables with validation."""
        try:
            security_protocol = SecurityProtocol(os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"))
        except ValueError as e:
            raise ValueError(f"Invalid security protocol: {e}")

        return cls(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "77.81.230.104:9092"),
            username=os.getenv("KAFKA_USERNAME", "admin"),
            password=os.getenv("KAFKA_PASSWORD", "VawEzo1ikLtrA8Ug8THa"),
            security_protocol=security_protocol,
            sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
            topic_prefix=os.getenv("KAFKA_TOPIC_PREFIX", "greenmoon_end"),
            num_partitions=int(os.getenv("KAFKA_NUM_PARTITIONS", "2")),
            replication_factor=int(os.getenv("KAFKA_REPLICATION_FACTOR", "1"))
        )

    def to_admin_config(self) -> Dict[str, str]:
        """Convert config to format expected by KafkaAdminClient."""
        sasl_plain_str = f'PLAIN\n{self.username}\n{self.password}'
        return {
            "bootstrap_servers": self.bootstrap_servers,
            "security_protocol": self.security_protocol.value,
            "sasl_mechanism": self.sasl_mechanism,
            "sasl_plain_username": self.username,
            "sasl_plain_password": self.password,
        }

class KafkaAdminManager:
    def __init__(self, config: KafkaConfig):
        self.config = config
        self.admin_client = KafkaAdminClient(**config.to_admin_config())

    def close(self):
        """Close the admin client connection."""
        if self.admin_client:
            self.admin_client.close()

    def delete_topics(self, topics: List[str]) -> None:
        """Deletes Kafka topics with error handling."""
        try:
            existing_topics = self.admin_client.list_topics()
            topics_to_delete = [topic for topic in topics if topic in existing_topics]
            
            if topics_to_delete:
                self.admin_client.delete_topics(topics_to_delete)
                for topic in topics_to_delete:
                    logger.info(f"Topic '{topic}' deleted successfully")
            else:
                logger.info("No existing topics to delete")
                
        except KafkaError as ke:
            logger.error(f"Kafka error during topic deletion: {ke}")
        except Exception as e:
            logger.error(f"Unexpected error during topic deletion: {e}")

    def create_topics(self, topics_config: Dict[str, str]) -> None:
        """Creates Kafka topics with error handling."""
        try:
            new_topics = [
                NewTopic(
                    name=topic,
                    num_partitions=self.config.num_partitions,
                    replication_factor=self.config.replication_factor
                )
                for topic in topics_config
            ]
            
            self.admin_client.create_topics(new_topics)
            for topic in topics_config:
                logger.info(f"Topic '{topic}' created successfully")
                
        except KafkaError as ke:
            logger.error(f"Kafka error during topic creation: {ke}")
        except Exception as e:
            logger.error(f"Unexpected error during topic creation: {e}")

    def list_topics(self) -> Optional[List[str]]:
        """Lists existing Kafka topics that match the prefix."""
        try:
            all_topics = self.admin_client.list_topics()
            matching_topics = [
                topic for topic in all_topics
                if self.config.topic_prefix in topic
            ]
            
            for topic in matching_topics:
                logger.info(f"Found existing topic: '{topic}'")
            return matching_topics
            
        except KafkaError as ke:
            logger.error(f"Kafka error during topic listing: {ke}")
        except Exception as e:
            logger.error(f"Unexpected error during topic listing: {e}")
        return None

def main():
    """Main execution function."""
    admin_manager = None
    try:
        # Load configuration
        config = KafkaConfig.from_env()
        
        # Define topics
        topics_to_create = {
            f"{config.topic_prefix}_athlete_event_results": "Topic for athlete event results",
            f"{config.topic_prefix}_enriched_athlete_avg": "Topic for enriched athlete averages"
        }

        # Initialize admin manager
        admin_manager = KafkaAdminManager(config)

        logger.info("Starting Kafka admin operations...")
        
        # Execute operations
        admin_manager.delete_topics(list(topics_to_create.keys()))
        admin_manager.create_topics(topics_to_create)
        admin_manager.list_topics()

    except Exception as e:
        logger.error(f"Failed to execute Kafka admin operations: {e}")
        raise
    finally:
        if admin_manager:
            admin_manager.close()

if __name__ == "__main__":
    main()