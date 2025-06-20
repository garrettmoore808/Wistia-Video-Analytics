import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import col, first, avg, count, max as max_, to_date, regexp_extract, input_file_name
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# S3 paths
BUCKET = "gmoore-wistia-project"
RAW_EVENTS = f"s3://{BUCKET}/raw/events/"
RAW_STATS = f"s3://{BUCKET}/raw/media_stats/"
RAW_ENGAGEMENT = f"s3://{BUCKET}/raw/media_engagement/"
OUTPUT = f"s3://{BUCKET}/processed/fact_media_engagement/"

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

# 1. Load events, stats, and engagement
df_events = spark.read.json(RAW_EVENTS)
df_stats = spark.read.json(RAW_STATS)
df_engagement = spark.read.json(RAW_ENGAGEMENT)

# 2. Prepare stats: Extract media_id from filename, keep latest per media
media_id_pattern = r"([a-z0-9]+)_\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}\.json"
df_stats_files = df_stats.withColumn("input_file_name", input_file_name())
df_stats_files = df_stats_files.withColumn(
    "media_id",
    regexp_extract(col("input_file_name"), media_id_pattern, 1)
)
window_stats = Window.partitionBy("media_id").orderBy(col("input_file_name").desc())
df_stats_latest = (
    df_stats_files
    .withColumn("rn", F.row_number().over(window_stats))
    .filter(col("rn") == 1)
    .select(
        "media_id",
        col("play_count").alias("media_play_count").cast("int"),
        col("play_rate").cast("double"),
        col("hours_watched").cast("double").alias("total_watch_time"),
    )
)

# 3. Prepare engagement: Extract media_id from filename, keep latest per media
df_engagement_files = df_engagement.withColumn("input_file_name", input_file_name())
df_engagement_files = df_engagement_files.withColumn(
    "media_id",
    regexp_extract(col("input_file_name"), media_id_pattern, 1)
)
df_engagement_latest = (
    df_engagement_files
    .withColumn("rn", F.row_number().over(window_stats))
    .filter(col("rn") == 1)
    .select(
        "media_id",
        col("engagement").alias("watched_percent_media")
    )
)

# 4. Prepare event facts: Group by media_id, visitor_id, and date
df_events_fact = (
    df_events
    .withColumn("date", to_date(col("received_at")))
    .groupBy("media_id", "visitor_key", "date")
    .agg(
        count("*").alias("event_play_count"),
        avg("percent_viewed").alias("watched_percent_event")
    )
    .withColumnRenamed("visitor_key", "visitor_id")
)

# 5. Join in stats and engagement (media-level)
df_fact = (
    df_events_fact
    .join(df_stats_latest, on="media_id", how="left")
    .join(df_engagement_latest, on="media_id", how="left")
    .select(
        df_events_fact["media_id"],
        df_events_fact["visitor_id"],
        df_events_fact["date"],
        df_events_fact["event_play_count"].alias("play_count"),
        df_stats_latest["play_rate"],
        df_stats_latest["total_watch_time"],
        df_events_fact["watched_percent_event"],
        df_engagement_latest["watched_percent_media"]
    )
)

# 6. Write to S3 as Parquet
df_fact.coalesce(1).write.mode("overwrite").parquet(OUTPUT)

print("✅ fact_media_engagement successfully written to", OUTPUT)
