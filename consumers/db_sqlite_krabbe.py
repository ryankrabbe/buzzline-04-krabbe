#####################################
# Import Modules
#####################################

# import from standard library
import sqlite3
import os
from pathlib import Path
import sys

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT / "consumers"))

from utils.utils_logger import logger

DB_FOLDER = Path(__file__).parent.parent / "data"
DB_FILE = DB_FOLDER / "soccer_data.db"

DB_FOLDER.mkdir(parents=True, exist_ok=True)

#####################################
# Connect to Database
#####################################

def get_db_connection():
    """
    Establish and return a database connection.
    """
    return sqlite3.connect(DB_FILE)

#####################################
# Create Tables
#####################################

def create_tables():
    """
    Create tables in the SQLite database.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # Match events table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            home_team TEXT,
            away_team TEXT,
            home_goals INTEGER,
            away_goals INTEGER,
            home_shots INTEGER,
            away_shots INTEGER,
            home_fouls INTEGER,
            away_fouls INTEGER
        )
    """)

    # Aggregate stats table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS team_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            team_name TEXT UNIQUE,
            matches_played INTEGER DEFAULT 0,
            total_goals INTEGER DEFAULT 0,
            total_shots INTEGER DEFAULT 0,
            total_fouls INTEGER DEFAULT 0
        )
    """)

    conn.commit()
    conn.close()
    logger.info("Database tables created successfully.")

#####################################
# Insert Match Data
#####################################

def insert_match(match_data):
    """
    Insert match data into the database.

    Args:
        match_data (dict): Match event details.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO matches (timestamp, home_team, away_team, home_goals, away_goals, home_shots, away_shots, home_fouls, away_fouls)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        match_data["timestamp"],
        match_data["home_team"],
        match_data["away_team"],
        match_data["home_goals"],
        match_data["away_goals"],
        match_data["home_shots"],
        match_data["away_shots"],
        match_data["home_fouls"],
        match_data["away_fouls"]
    ))

    conn.commit()
    conn.close()
    logger.info(f"Inserted match: {match_data['home_team']} vs {match_data['away_team']}")

#####################################
# Update Team Stats
#####################################

def update_team_stats(team_name, goals, shots, fouls):
    """
    Update or insert team statistics.

    Args:
        team_name (str): Name of the team.
        goals (int): Goals scored.
        shots (int): Shots taken.
        fouls (int): Fouls committed.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # Check if team exists
    cursor.execute("SELECT * FROM team_stats WHERE team_name = ?", (team_name,))
    team = cursor.fetchone()

    if team:
        cursor.execute("""
            UPDATE team_stats
            SET matches_played = matches_played + 1,
                total_goals = total_goals + ?,
                total_shots = total_shots + ?,
                total_fouls = total_fouls + ?
            WHERE team_name = ?
        """, (goals, shots, fouls, team_name))
    else:
        cursor.execute("""
            INSERT INTO team_stats (team_name, matches_played, total_goals, total_shots, total_fouls)
            VALUES (?, 1, ?, ?, ?)
        """, (team_name, goals, shots, fouls))

    conn.commit()
    conn.close()
    logger.info(f"Updated stats for {team_name}")

#####################################
# Initialize Database
#####################################

if __name__ == "__main__":
    create_tables()
