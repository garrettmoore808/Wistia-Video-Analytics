import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import col, first

BUCKET = "gmoore-wistia-project"
RAW_EVENTS = f"s3://{BUCKET}/raw/events/"
OUTPUT = f"s3://{BUCKET}/processed/dim_visitor/"

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

df_events = spark.read.json(RAW_EVENTS)

# One row per visitor_id, with first ip/country seen
df_dim_visitor = (
    df_events
    .groupBy("visitor_key")
    .agg(
        first("ip", ignorenulls=True).alias("ip_address"),
        first("country", ignorenulls=True).alias("country"),
    )
    .withColumnRenamed("visitor_key", "visitor_id")
)

df_dim_visitor.coalesce(1).write.mode("overwrite").parquet(OUTPUT)

print("✅ dim_visitor successfully written to", OUTPUT)
