import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import col, explode, when, row_number, regexp_extract, input_file_name
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# S3 locations
BUCKET = "gmoore-wistia-project"
RAW_METADATA = f"s3://{BUCKET}/raw/media_metadata/"
RAW_STATS = f"s3://{BUCKET}/raw/media_stats/"
OUTPUT = f"s3://{BUCKET}/processed/dim_media/"

# Spark/Glue setup
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

# 1. Load metadata and stats
df_meta = spark.read.json(RAW_METADATA)
df_stats = spark.read.json(RAW_STATS)

# 2. Extract media_id from media_stats filenames
media_id_pattern = r"([a-z0-9]+)_\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}\.json"
df_stats_files = df_stats.withColumn("input_file_name", input_file_name())
df_stats_files = df_stats_files.withColumn(
    "media_id",
    regexp_extract(col("input_file_name"), media_id_pattern, 1)
)
df_stats_clean = df_stats_files.withColumnRenamed("visitors", "visitors_count")

# 3. Explode assets and rank for best video URL
assets_exploded = df_meta.withColumn("asset", explode("assets"))
assets_ranked = assets_exploded.withColumn(
    "asset_rank",
    when(col("asset.type") == "Mp4VideoFile", 1)
    .when(col("asset.type") == "HdMp4VideoFile", 2)
    .when(col("asset.type") == "OriginalFile", 3)
    .otherwise(4)
)
window_asset = Window.partitionBy("hashed_id").orderBy("asset_rank")
df_meta_clean = (
    assets_ranked
    .withColumn("row_num", row_number().over(window_asset))
    .filter(col("row_num") == 1)
    .select(
        col("hashed_id").alias("media_id"),
        col("id").alias("media_sk"),
        col("name").alias("title"),
        col("asset.url").alias("url"),
        col("project.name").alias("channel"),
        col("created").alias("created_at"),
    )
)

# 4. Join metadata and stats using media_id
df = df_meta_clean.join(
    df_stats_clean,
    on="media_id",
    how="inner"
)

# 5. Keep only the latest stats file for each media_id
window_stats = Window.partitionBy("media_id").orderBy(F.desc("input_file_name"))
df_with_rn = df.withColumn("rn", F.row_number().over(window_stats))
df_latest = df_with_rn.filter(col("rn") == 1)

# 6. Select and cast final columns
df_final = df_latest.select(
    "media_id",
    "title",
    "url",
    "channel",
    "created_at",
    col("play_count").cast("int"),
    col("play_rate").cast("double"),
    col("hours_watched").cast("double"),
    col("engagement").cast("double"),
    col("visitors_count").cast("int"),
    col("load_count").cast("int")
)

# 7. Write to Parquet in processed folder
df_final.coalesce(1).write.mode("overwrite").parquet(OUTPUT)

print("✅ dim_media successfully written to", OUTPUT)
