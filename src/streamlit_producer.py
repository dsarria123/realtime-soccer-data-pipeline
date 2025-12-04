import os
import json
import time
import requests
import pandas as pd
from kafka import KafkaProducer
import streamlit as st

# === Global Config Variables ===
API_KEY = os.getenv("API_KEY")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
CSV_OUTPUT_PATH = os.getenv("CSV_OUTPUT_PATH", "data/demo_stats.csv")

# === Kafka Producer ===
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# === Functions ===
def get_live_fixture_ids():
    """Fetch all currently live fixture IDs from the API."""
    url = "https://v3.football.api-sports.io/fixtures?live=all"
    headers = {"x-apisports-key": API_KEY}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    data = resp.json().get("response", [])
    return [fixture["fixture"]["id"] for fixture in data]

def build_fixture_dataframe(fixture_id):
    """Fetch fixture info, stats, and odds, return as a single DataFrame."""
    headers = {"x-apisports-key": API_KEY}

    # --- Fixture Info ---
    fixture_url = f"https://v3.football.api-sports.io/fixtures?id={fixture_id}"
    fixture_resp = requests.get(fixture_url, headers=headers)
    fixture_resp.raise_for_status()
    fixture_data = fixture_resp.json()["response"][0]

    # --- Stats ---
    stats_url = "https://v3.football.api-sports.io/fixtures/statistics"
    stats_resp = requests.get(stats_url, headers=headers, params={"fixture": fixture_id})
    stats_resp.raise_for_status()
    stats_data = stats_resp.json().get("response", [])

    stats_flat = {}
    for team_stat in stats_data:
        team = team_stat["team"]["name"]
        for stat in team_stat["statistics"]:
            key = f"{team}_{stat['type']}"
            stats_flat[key] = stat["value"]

    # --- Odds ---
    odds_url = "https://v3.football.api-sports.io/odds/live"
    odds_resp = requests.get(odds_url, headers=headers, params={"fixture": fixture_id})
    odds_data = odds_resp.json().get("response", [])

    odds_map = {}
    try:
        if odds_data and "odds" in odds_data[0]:
            for bet in odds_data[0]["odds"]:
                if bet["name"] in ["Fulltime Result", "1x2"]:
                    for val in bet["values"]:
                        odds_map[f"Odds_{val['value']}"] = val["odd"]
    except Exception as e:
        st.warning(f"Odds parsing error: {e}")

    # --- Core Fixture Info ---
    match_info = {
        "fixture_id": fixture_data["fixture"]["id"],
        "league": fixture_data["league"]["name"],
        "country": fixture_data["league"]["country"],
        "home_team": fixture_data["teams"]["home"]["name"],
        "away_team": fixture_data["teams"]["away"]["name"],
        "home_score": fixture_data["goals"]["home"],
        "away_score": fixture_data["goals"]["away"],
        "status": fixture_data["fixture"]["status"]["long"],
        "elapsed": fixture_data["fixture"]["status"]["elapsed"],
        "timestamp": fixture_data["fixture"]["date"]
    }

    combined = {**match_info, **stats_flat, **odds_map}
    return pd.DataFrame([combined])

def send_to_kafka(df):
    """Send the first record in the DataFrame to Kafka."""
    producer.send(KAFKA_TOPIC, value=df.to_dict(orient="records")[0])

def save_to_csv(df):
    """Append the DataFrame to a CSV file."""
    os.makedirs(os.path.dirname(CSV_OUTPUT_PATH), exist_ok=True)
    df.to_csv(CSV_OUTPUT_PATH, mode='a', header=not os.path.exists(CSV_OUTPUT_PATH), index=False)

# === Streamlit App ===
st.title("Real-Time Soccer Match Producer")

# Input fixture ID
fixture_id_input = st.text_input("Enter Fixture ID (or leave blank to fetch first live match)")

if st.button("Fetch and Stream"):
    try:
        # Determine fixture ID to fetch
        if fixture_id_input:
            fixture_id = int(fixture_id_input)
        else:
            live_ids = get_live_fixture_ids()
            if not live_ids:
                st.warning("No live fixtures currently.")
                st.stop()
            fixture_id = live_ids[0]

        # Fetch data
        df = build_fixture_dataframe(fixture_id)

        # Display DataFrame
        st.subheader(f"Match Data for Fixture {fixture_id}")
        st.dataframe(df)

        # Send to Kafka
        send_to_kafka(df)
        st.success(f"Sent data to Kafka topic '{KAFKA_TOPIC}'.")

        # Save to CSV
        save_to_csv(df)
        st.success(f"Appended data to CSV at '{CSV_OUTPUT_PATH}'.")

    except Exception as e:
        st.error(f"Error fetching or streaming data: {e}")
