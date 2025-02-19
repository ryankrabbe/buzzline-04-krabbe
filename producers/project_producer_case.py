"""
project_producer_case.py

Stream JSON soccer match data from a file to Kafka.
"""

#####################################
# Import Modules
#####################################

import json
import os
import random
import time
import pathlib
from datetime import datetime
from dotenv import load_dotenv

# Import Kafka only if available
try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

# Import logging utility
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for Environment Variables
#####################################

def get_message_interval() -> int:
    return int(os.getenv("PROJECT_INTERVAL_SECONDS", 5))

def get_kafka_topic() -> str:
    return os.getenv("PROJECT_TOPIC", "soccer_matches")

def get_kafka_server() -> str:
    return os.getenv("KAFKA_SERVER", "localhost:9092")

#####################################
# Set up Paths
#####################################

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
DATA_FOLDER = PROJECT_ROOT.joinpath("data")
MATCHES_FILE = DATA_FOLDER.joinpath("soccer_matches.json")

#####################################
# Load Match Data
#####################################

def load_matches():
    """
    Load match data from the JSON file.
    """
    try:
        with MATCHES_FILE.open("r") as f:
            matches = json.load(f)
        return matches
    except Exception as e:
        logger.error(f"Error loading match data: {e}")
        return []

#####################################
# Main Function to Stream Data
#####################################

def stream_matches():
    """
    Read soccer matches from a file and stream to Kafka.
    """
    logger.info("START soccer match producer...")
    interval_secs = get_message_interval()
    topic = get_kafka_topic()
    kafka_server = get_kafka_server()

    # Attempt to create Kafka producer
    producer = None
    if KAFKA_AVAILABLE:
        try:
            producer = KafkaProducer(
                bootstrap_servers=kafka_server,
                value_serializer=lambda x: json.dumps(x).encode("utf-8")
            )
            logger.info(f"Kafka producer connected to {kafka_server}")
        except Exception as e:
            logger.error(f"Kafka connection failed: {e}")
            producer = None

    matches = load_matches()
    if not matches:
        logger.error("No matches found in the file.")
        return

    try:
        while True:
            for match in matches:
                match["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                logger.info(f"Streaming match: {match}")

                # Send match to Kafka
                if producer:
                    producer.send(topic, value=match)
                    logger.info(f"Sent match data to Kafka topic '{topic}': {match}")

                time.sleep(interval_secs)

    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        if producer:
            producer.close()
            logger.info("Kafka producer closed.")
        logger.info("Producer shutting down.")

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    stream_matches()