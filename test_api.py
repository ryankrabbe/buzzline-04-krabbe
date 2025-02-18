import requests
import os
from dotenv import load_dotenv

# Load API Key
load_dotenv()
API_KEY = os.getenv("API_FOOTBALL_KEY")

# Define API URL for live Premier League matches
API_URL = "https://v3.football.api-sports.io/fixtures?league=39&season=2023&live=all"

HEADERS = {
    "X-RapidAPI-Key": API_KEY,
    "X-RapidAPI-Host": "v3.football.api-sports.io"
}

# Make API Request
response = requests.get(API_URL, headers=HEADERS)

if response.status_code == 200:
    print("✅ API is working! Here's a sample response:")
    print(response.json())  # Print a portion of the response
else:
    print(f"❌ API request failed with status code {response.status_code}.")
    print(f"Error details: {response.text}")
