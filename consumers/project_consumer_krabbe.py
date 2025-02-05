"""
project_consumer_krabbe.py

Consume json messages from a Kafka topic and visualize author counts in real-time.

JSON is a set of key:value pairs. 

Example serialized Kafka message
"{\"message\": \"I love Python!\", \"author\": \"Eve\"}"

Example JSON message (after deserialization) to be analyzed
{"message": "I love Python!", "author": "Eve"}

"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import json  # handle JSON parsing
import collections
from collections import defaultdict  # data structure for counting author occurrences
import sys

# Import external packages
from dotenv import load_dotenv
from kafka import KafkaConsumer

# IMPORTANT
# Import Matplotlib.pyplot for live plotting
# Use the common alias 'plt' for Matplotlib.pyplot
# Know pyplot well
import matplotlib.pyplot as plt

# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer
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


def get_kafka_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("SOCCER_CONSUMER_GROUP", "soccer_group_1")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


#####################################
# Set up data structures
#####################################

# Initialize a dictionary to store author counts
author_counts = defaultdict(int)

#####################################
# Set up live visuals
match_stats = collections.defaultdict(lambda: {"matches": 0, "goals": 0, "shots": 0, "fouls": 0})

# Use the subplots() method to create a tuple containing
# two objects at once:
# - a figure (which can have many axis)
# - an axis (what they call a chart in Matplotlib)
fig, ax = plt.subplots()

# Use the ion() method (stands for "interactive on")
# to turn on interactive mode for live updates
plt.ion()

#####################################
# Define an update chart function for live plotting
# This will get called every time a new message is processed
#####################################


def update_chart():
    """Update the live chart with the latest author counts."""
    # Clear the previous chart
    ax.clear()

    # Get the teams and counts from the dictionary
    teams = list(match_stats.keys())
    goals = [match_stats[team]["goals"] for team in teams]
    shots = [match_stats[team]["shots"] for team in teams]
    fouls = [match_stats[team]["fouls"] for team in teams]

    bar_width = 0.25
    x_positions = range(len(teams))

    # Create a bar chart using the bar() method.
    # Pass in the x list, the y list, and the color
    ax.bar(x_positions, goals, width=bar_width, label="Goals", color="green")
    ax.bar([x + bar_width for x in x_positions], shots, width=bar_width, label="Shots", color="blue")
    ax.bar([x + 2 * bar_width for x in x_positions], fouls, width=bar_width, label="Fouls", color="red")

    # Use the built-in axes methods to set the labels and title
    ax.set_xlabel("Teams")
    ax.set_ylabel("Count")
    ax.set_title("Live Soccer Match Statistics - Ryan Krabbe")

    # Use the set_xticklabels() method to rotate the x-axis labels
    # Pass in the x list, specify the rotation angle is 45 degrees,
    # and align them to the right
    # ha stands for horizontal alignment
    ax.set_xticks([x + bar_width for x in x_positions])
    ax.set_xticklabels(teams, rotation=45, ha="right")
    ax.legend()

    # Use the tight_layout() method to automatically adjust the padding
    plt.tight_layout()

    # Draw the chart
    plt.draw()

    # Pause briefly to allow some time for the chart to render
    plt.pause(0.01)


#####################################
# Function to process a match event
# #####################################


def process_match_event(match_event):
    """
    Process an incoming soccer match event.

    Args:
        match_event (dict): The match event data.
    """
    home_team = match_event["home_team"]
    away_team = match_event["away_team"]

    # Update stats for both teams
    for team, goals, shots, fouls in [
        (home_team, match_event["home_goals"], match_event["home_shots"], match_event["home_fouls"]),
        (away_team, match_event["away_goals"], match_event["away_shots"], match_event["away_fouls"]),
    ]:
        match_stats[team]["matches"] += 1
        match_stats[team]["goals"] += goals
        match_stats[team]["shots"] += shots
        match_stats[team]["fouls"] += fouls

    logger.info(f"Updated stats: {team} -> {match_stats[team]}")


#####################################
# Define main function for this module
#####################################


def main() -> None:
    """
    Main entry point for the consumer.

    - Listens for soccer match messages from Kafka.
    - Processes the match events in real-time.
    """
    logger.info("START soccer match consumer.")

    # fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_group_id()

    # Create the Kafka consumer
    try:
        consumer = KafkaConsumer(
            topic,
            group_id=group_id,
            bootstrap_servers=os.getenv("KAFKA_BROKER", "localhost:9092"),
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )
        logger.info(f"Listening for messages on topic '{topic}'...")
    except Exception as e:
        logger.error(f"Failed to create Kafka consumer: {e}")
        sys.exit(1)

    try:
        for message in consumer:
            match_event = message.value
            logger.info(f"Received match event: {match_event}")
            process_match_event(match_event)
            update_chart()
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error during message consumption: {e}")
    finally:
        consumer.close()
        logger.info("Kafka consumer closed.")

    logger.info("END soccer match consumer.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
