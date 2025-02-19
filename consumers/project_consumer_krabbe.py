# Import modules
import json
import os
import sys
import numpy as np
from collections import deque
from pathlib import Path
import collections
import matplotlib.pyplot as plt
from kafka import KafkaConsumer
from dotenv import load_dotenv
from datetime import datetime
from utils.utils_logger import logger

# IMPORTANT
# Import Matplotlib.pyplot for live plotting
# Use the common alias 'plt' for Matplotlib.pyplot
# Know pyplot well
import matplotlib.pyplot as plt

# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

# Ensure `consumers/` is in sys.path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT))
sys.path.append(str(PROJECT_ROOT / "consumers"))

from consumers.db_sqlite_krabbe import insert_match, update_team_stats

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################

def get_kafka_topic() -> str:
    return os.getenv("PROJECT_TOPIC", "soccer_matches")

def get_kafka_group_id() -> str:
    return os.getenv("SOCCER_CONSUMER_GROUP", "soccer_group_1")

#####################################
# Set up live visuals
match_stats = collections.defaultdict(lambda: {"matches": 0, "goals": 0, "shots": 0, "fouls": 0})

fig, ax = plt.subplots()
plt.ion()

#####################################
# Define an update chart function for live plotting
#####################################

MATCH_HISTORY_LIMIT = 5
team_history = collections.defaultdict(lambda: {"goals": deque(maxlen=MATCH_HISTORY_LIMIT),
                                                "shots": deque(maxlen=MATCH_HISTORY_LIMIT),
                                                "fouls": deque(maxlen=MATCH_HISTORY_LIMIT)})

def update_chart():
    """Update the live statistics chart."""
    ax.clear()

    teams_sorted = sorted(match_stats.keys(), key=lambda x: match_stats[x]["goals"], reverse=True)
    goals = [match_stats[team]["goals"] for team in teams_sorted]
    shots = [match_stats[team]["shots"] for team in teams_sorted]
    fouls = [match_stats[team]["fouls"] for team in teams_sorted]

    # Update moving averages
    moving_avg_goals = []
    moving_avg_shots = []
    moving_avg_fouls = []

    for team in teams_sorted:
        team_history[team]["goals"].append(match_stats[team]["goals"])
        team_history[team]["shots"].append(match_stats[team]["shots"])
        team_history[team]["fouls"].append(match_stats[team]["fouls"])

        moving_avg_goals.append(np.mean(team_history[team]["goals"]))
        moving_avg_shots.append(np.mean(team_history[team]["shots"]))
        moving_avg_fouls.append(np.mean(team_history[team]["fouls"]))

    bar_width = 0.25
    x_positions = np.arange(len(teams_sorted))

    # Bar Chart for Current Stats
    ax.bar(x_positions, goals, width=bar_width, label="Goals", color="#2E86C1", alpha=0.9)
    ax.bar(x_positions + bar_width, shots, width=bar_width, label="Shots", color="#28B463", alpha=0.8)
    ax.bar(x_positions + 2 * bar_width, fouls, width=bar_width, label="Fouls", color="#E74C3C", alpha=0.8)

    # Overlay Line Chart for Moving Averages
    ax.plot(x_positions, moving_avg_goals, marker="o", linestyle="-", color="#1F618D", label="Avg Goals", linewidth=2)
    ax.plot(x_positions, moving_avg_shots, marker="o", linestyle="-", color="#117A65", label="Avg Shots", linewidth=2)
    ax.plot(x_positions, moving_avg_fouls, marker="o", linestyle="-", color="#943126", label="Avg Fouls", linewidth=2)

    ax.set_xlabel("Teams", fontsize=12, fontweight='bold')
    ax.set_ylabel("Count", fontsize=12, fontweight='bold')
    ax.set_title("Live Soccer Match Statistics - Ryan Krabbe", fontsize=14, fontweight='bold')
    ax.set_xticks(x_positions + bar_width)
    ax.set_xticklabels(teams_sorted, rotation=45, ha="right", fontsize=10)
    ax.legend(fontsize=10)
    ax.grid(axis="y", linestyle="--", alpha=0.7)

    plt.tight_layout()
    plt.draw()
    plt.pause(0.01)


#####################################
# Function to process a match event
# #####################################


def process_match_event(match_event):
    """
    Process an incoming soccer match event.
    """
    home_team = match_event["home_team"]
    away_team = match_event["away_team"]
    home_goals = match_event["home_goals"]
    away_goals = match_event["away_goals"]
    home_shots = match_event["home_shots"]
    away_shots = match_event["away_shots"]
    home_fouls = match_event["home_fouls"]
    away_fouls = match_event["away_fouls"]

    # Store match data in SQLite
    match_data = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "home_team": home_team,
        "away_team": away_team,
        "home_goals": home_goals,
        "away_goals": away_goals,
        "home_shots": home_shots,
        "away_shots": away_shots,
        "home_fouls": home_fouls,
        "away_fouls": away_fouls
    }

    insert_match(match_data)

    # Update live stats visualization
    for team, goals, shots, fouls in [
        (home_team, home_goals, home_shots, home_fouls),
        (away_team, away_goals, away_shots, away_fouls),
    ]:
        match_stats[team]["matches"] += 1
        match_stats[team]["goals"] += goals
        match_stats[team]["shots"] += shots
        match_stats[team]["fouls"] += fouls

    logger.info(f"Updated stats: {team} -> {match_stats[team]}")


#####################################
# Define main function for this module
#####################################


def main():
    logger.info("START soccer match consumer.")

    topic = get_kafka_topic()
    group_id = get_kafka_group_id()

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
    finally:
        consumer.close()
        logger.info("Kafka consumer closed.")

if __name__ == "__main__":
    main()