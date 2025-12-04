from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, regexp_replace, expr, when, lit, lower
from pyspark.sql.types import StringType, DoubleType, IntegerType, StructType, MapType
import os
import pandas as pd

# === Global Config Variables ===
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
CSV_OUTPUT_PATH = os.getenv("CSV_OUTPUT_PATH", "data/demo_stats.csv")

# === Spark session ===
spark = SparkSession.builder.appName("FootballConsumer").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# === Schema for core fields only ===
base_schema = StructType() \
    .add("fixture_id", StringType()) \
    .add("league", StringType()) \
    .add("country", StringType()) \
    .add("home_team", StringType()) \
    .add("away_team", StringType()) \
    .add("home_score", IntegerType()) \
    .add("away_score", IntegerType()) \
    .add("status", StringType()) \
    .add("elapsed", IntegerType()) \
    .add("timestamp", StringType()) \
    .add("odds_home", StringType()) \
    .add("odds_draw", StringType()) \
    .add("odds_away", StringType())

# === Read from Kafka ===
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .load()

# === Extract and clean JSON ===
json_df = raw_df.selectExpr("CAST(value AS STRING) AS raw_json") \
    .withColumn("data", from_json(col("raw_json"), base_schema)) \
    .select("data.*", "raw_json")

# Clean team names
json_df = json_df.withColumn("home_team_clean", lower(regexp_replace(col("home_team"), " ", "_"))) \
                 .withColumn("away_team_clean", lower(regexp_replace(col("away_team"), " ", "_")))

# Parse full JSON into a map
json_df = json_df.withColumn("kv_map", from_json(col("raw_json"), MapType(StringType(), StringType())))

# Stat suffixes
stat_suffixes = [
    "shots_on_goal", "shots_off_goal", "total_shots", "blocked_shots", "shots_insidebox",
    "shots_outsidebox", "fouls", "corner_kicks", "offsides", "ball_possession", "yellow_cards",
    "red_cards", "goalkeeper_saves", "total_passes", "passes_accurate", "passes_%",
    "expected_goals", "goals_prevented"
]

# Extract stats
for suffix in stat_suffixes:
    json_df = json_df.withColumn(f"home_{suffix}", expr(f"kv_map[concat(home_team_clean, '_{suffix}')]"))
    json_df = json_df.withColumn(f"away_{suffix}", expr(f"kv_map[concat(away_team_clean, '_{suffix}')]"))

# Cast numeric stats
for stat in stat_suffixes:
    if stat not in ["ball_possession", "passes_%"]:
        json_df = json_df.withColumn(f"home_{stat}", col(f"home_{stat}").cast(DoubleType()))
        json_df = json_df.withColumn(f"away_{stat}", col(f"away_{stat}").cast(DoubleType()))

# Derived stats
final_df = json_df \
    .withColumn("home_pass_accuracy", when(col("home_total_passes") > 0,
                                           col("home_passes_accurate") / col("home_total_passes")).otherwise(lit(None))) \
    .withColumn("away_pass_accuracy", when(col("away_total_passes") > 0,
                                           col("away_passes_accurate") / col("away_total_passes")).otherwise(lit(None))) \
    .withColumn("odds_home", col("odds_home").cast(DoubleType())) \
    .withColumn("odds_away", col("odds_away").cast(DoubleType())) \
    .withColumn("xg_diff", col("home_expected_goals") - col("away_expected_goals")) \
    .withColumn("odds_diff", col("odds_home") - col("odds_away"))

# === Output to console ===
console_query = final_df.select(
    "fixture_id", "home_team", "away_team", "elapsed", "status",
    "home_score", "away_score", "home_pass_accuracy", "away_pass_accuracy",
    "xg_diff", "odds_diff"
).writeStream.format("console").outputMode("append").option("truncate", False).start()

# === Append to CSV ===
def append_to_csv(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    pdf = batch_df.toPandas()
    cols = sorted([col for col in pdf.columns if not col.startswith("raw_json") and not col.endswith("_clean") and col != "kv_map"])
    pdf = pdf[cols]
    os.makedirs(os.path.dirname(CSV_OUTPUT_PATH), exist_ok=True)
    pdf.to_csv(CSV_OUTPUT_PATH, mode='a', header=not os.path.exists(CSV_OUTPUT_PATH), index=False)

csv_query = final_df.writeStream.foreachBatch(append_to_csv).outputMode("append").start()

console_query.awaitTermination()
csv_query.awaitTermination()
