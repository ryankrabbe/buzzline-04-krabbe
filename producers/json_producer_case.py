"""
json_producer_case.py

Stream JSON data to a Kafka topic.

Example JSON message
{"message": "I love Python!", "author": "Eve"}

Example serialized to Kafka message
"{\"message\": \"I love Python!\", \"author\": \"Eve\"}"

"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import sys
import time
import pathlib  # work with file paths
import json  # work with JSON data
import random

# Import external packages
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,
)
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("SOCCER_MATCH_TOPIC", "soccer_matches")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_message_interval() -> int:
    """Fetch message interval from environment or use default."""
    interval = int(os.getenv("MATCH_INTERVAL_SECONDS", 3))
    logger.info(f"Message interval: {interval} seconds")
    return interval


#####################################
# Set up Paths
#####################################

# The parent directory of this file is its folder.
# Go up one more parent level to get the project root.
PROJECT_ROOT = pathlib.Path(__file__).parent.parent
logger.info(f"Project root: {PROJECT_ROOT}")

# Set directory where data is stored
DATA_FOLDER: pathlib.Path = PROJECT_ROOT.joinpath("data")
logger.info(f"Data folder: {DATA_FOLDER}")

# Set the name of the data file
DATA_FILE: pathlib.Path = DATA_FOLDER.joinpath("project.json")
logger.info(f"Data file: {DATA_FILE}")

#####################################
# Soccer Match Generator
#####################################

# Teams for Matches
TEAMS = [
    "Manchester United",
    "Chelsea",
    "Liverpool",
    "Arsenal",
    "Manchester City",
    "Tottenham",
    "Newcastle United",
    "Aston Villa",
    "West Ham",
    "Brighton",
]

def generate_match_event():
    """
    Generate a random soccer match event.

    Returns:
        dict: A dictionary containing match details.
    """
    home_team, away_team = random.sample(TEAMS, 2)
    home_possession = random.randint(40, 60)
    away_possession = 100 - home_possession

    match_data = {
        "match_id": random.randint(100, 999),
        "date": time.strftime("%Y-%m-%d"),
        "home_team": home_team,
        "away_team": away_team,
        "home_goals": random.randint(0, 5),
        "away_goals": random.randint(0, 5),
        "home_possession": home_possession,
        "away_possession": away_possession,
        "home_shots": random.randint(5, 20),
        "away_shots": random.randint(5, 20),
        "home_fouls": random.randint(5, 15),
        "away_fouls": random.randint(5, 15),
    }

    logger.debug(f"Generated match event: {match_data}")
    return match_data

#####################################
# Main Function
#####################################


def main():
    """
    Main entry point for this producer.

    - Ensures the Kafka topic exists.
    - Creates a Kafka producer using the `create_kafka_producer` utility.
    - Streams generated soccer match JSON messages to the Kafka topic.
    """

    logger.info("START soccer match producer.")
    verify_services()

    # fetch .env content
    topic = get_kafka_topic()
    interval_secs = get_message_interval()

    # Verify the data file exists
    if not DATA_FILE.exists():
        logger.error(f"Data file not found: {DATA_FILE}. Exiting.")
        sys.exit(1)

    # Create the Kafka producer
    producer = create_kafka_producer(
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
    )
    if not producer:
        logger.error("Failed to create Kafka producer. Exiting...")
        sys.exit(3)

    # Create topic if it doesn't exist
    try:
        create_kafka_topic(topic)
        logger.info(f"Kafka topic '{topic}' is ready.")
    except Exception as e:
        logger.error(f"Failed to create or verify topic '{topic}': {e}")
        sys.exit(1)

    # Generate and send match events
    logger.info(f"Starting soccer match production to topic '{topic}'...")
    try:
        while True:
            match_event = generate_match_event()
            producer.send(topic, value=match_event)
            logger.info(f"Sent match event to topic '{topic}': {match_event}")
            time.sleep(interval_secs)
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Error during match event production: {e}")
    finally:
        producer.close()
        logger.info("Kafka producer closed.")

    logger.info("END soccer match producer.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
