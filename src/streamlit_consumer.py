import os
import pandas as pd
import streamlit as st

# === Global Config Variables ===
CSV_OUTPUT_PATH = os.getenv("CSV_OUTPUT_PATH", "data/demo_stats.csv")

# === Streamlit App ===
st.title("Real-Time Soccer Match Consumer")

# Check if CSV exists
if not os.path.exists(CSV_OUTPUT_PATH):
    st.warning(f"No data found at {CSV_OUTPUT_PATH}. Run the producer and consumer first.")
    st.stop()

# Load CSV
df = pd.read_csv(CSV_OUTPUT_PATH)

# Select fixture to view
fixture_ids = df['fixture_id'].unique()
selected_fixture = st.selectbox("Select Fixture ID", fixture_ids)

# Filter dataframe
fixture_df = df[df['fixture_id'] == selected_fixture]

st.subheader(f"Stats for Fixture {selected_fixture}")
st.dataframe(fixture_df)

# Optional: basic plots
if st.checkbox("Show Score Progression"):
    st.line_chart(fixture_df[['home_score', 'away_score']])

if st.checkbox("Show xG & Odds Difference"):
    st.line_chart(fixture_df[['xg_diff', 'odds_diff']])
