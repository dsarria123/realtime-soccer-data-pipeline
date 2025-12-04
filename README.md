# Real-Time Soccer Match Data Streaming (Python, Spring 2025)

This project implements a real-time ingestion pipeline that retrieves live soccer match data from an external API, normalizes the responses, and streams structured records into Kafka. It demonstrates practical skills in API-driven data collection, real-time streaming, building pipelines suitable for downstream analytics, and is deployed on Streamlit for interactive visualization.

---

## Project Objectives

- Normalize varying API responses into a consistent, analysis-ready schema.
- Stream event-level data in real time rather than capturing periodic snapshots.
- Apply principles used in modern data engineering workflows, including modular extraction, transformation, and transport.
- Demonstrate the ability to refine parsing logic when external data sources change or return inconsistent fields.
- Visualize live and historical match data using Streamlit for rapid inspection and analysis.

---

## Technologies Used

- **Python**
- **Requests** for API ingestion
- **Kafka** for real-time streaming
- **Pandas** for data manipulation
- **PySpark** for streaming consumption and processing
- **Streamlit** for interactive dashboards
- **JSON normalization** and schema design

---
## Running the Project

This project can be executed entirely on a local machine with Python and Kafka installed. It was originally run on the NC State University HPC cluster for testing, but HPC is not required.

### 1. Setup

1. Install Python dependencies listed in `requirements.txt`:

```bash
pip install -r requirements.txt
```
2. Ensure Kafka is running locally (or update KAFKA_BOOTSTRAP_SERVERS to point to your broker).
3. Configure your environment variables in a .env file or system environment, based on config/example_config.json.

### 2. Running the Producer (Streamlit)

```bash
streamlit run src/streamlit_producer.py
```

- Enter a specific fixture ID or leave blank to fetch the first live match.
- Click Fetch and Stream to send data to Kafka and append it to a CSV file.
- The producer also displays the structured match data in real time.

### 3. Running the Consumer (Spark)

```bash
python src/consumer_spark.py
```

- Consumes data from Kafka in real time.
- Writes processed data to a CSV file in data/.
- Outputs a live console preview of key match statistics.

### 4. Viewing Processed Data (Streamlit)
```bash
streamlit run src/streamlit_consumer.py
```

- Provides an interactive interface to explore historical match data stored in the CSV.
- Displays tables and charts for key statistics, such as expected goals, possession, and odds differences.
