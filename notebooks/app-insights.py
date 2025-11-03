# Databricks notebook source
# MAGIC %md
# MAGIC # App Insights ETL Pipeline
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook implements a **medallion architecture** (Bronze → Silver → Gold) for processing app insights data.
# MAGIC
# MAGIC ## Pipeline Characteristics
# MAGIC
# MAGIC | Property | Value |
# MAGIC |----------|-------|
# MAGIC | **Architecture** | Medallion (Bronze → Silver → Gold) |
# MAGIC | **Idempotency** | Yes - partition overwrite by snapshot_date |
# MAGIC | **Data Quality** | Validation + Quarantine table for failed records |
# MAGIC | **Partitioning** | By snapshot_date for all layers |
# MAGIC | **Deduplication** | By slug (natural key) keeping highest popularity |
# MAGIC | **Processing Mode** | Batch - daily snapshots |
# MAGIC
# MAGIC ## Data Layers
# MAGIC
# MAGIC **Bronze Layer:**
# MAGIC - Raw JSON ingestion from volumes
# MAGIC - Deduplication by slug
# MAGIC - Quality validation and quarantine
# MAGIC
# MAGIC **Silver Layer:**
# MAGIC - Cleaned and validated records
# MAGIC - Normalized schema with surrogate keys
# MAGIC - Flattened nested structures
# MAGIC - Null handling and type conversions
# MAGIC
# MAGIC **Gold Layer:**
# MAGIC - Aggregated business metrics
# MAGIC - Daily app-level summaries
# MAGIC - Category-level analytics
# MAGIC
# MAGIC ## Quality Checks
# MAGIC
# MAGIC - Duplicate detection and removal
# MAGIC - Null value validation
# MAGIC - Negative metric detection
# MAGIC - Quarantine table for problematic records
# MAGIC ## Tables Created
# MAGIC
# MAGIC | Layer | Table | Description |
# MAGIC |-------|-------|-------------|
# MAGIC | Silver | `silver_apps_daily_snapshot` | Cleaned app records |
# MAGIC | Bronze | `bronze_apps_quarantine` | Records failing quality checks |
# MAGIC | Gold | `gold_app_metrics_daily` | Daily aggregated metrics |
# MAGIC | Gold | `gold_category_metrics_daily` | Category-level metrics |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from pyspark.sql.functions import (
    coalesce,
    sha2,
    concat_ws,
    transform,
    col,
    count,
    sum as sql_sum,
    avg,
    lit,
    when,
    length,
    current_timestamp,
    to_date,
    size,
    array_sort,
    desc,
    row_number,
    expr,
)
from pyspark.sql.window import Window

# COMMAND ----------

available_dates_raw = dbutils.fs.ls(
    "/Volumes/interview_data_pde/app_insights_assignment/raw_apps/"
)

# Parse directory names (YYYYMMDD format)
available_dates_yyyymmdd = sorted([
    d.name.rstrip('/')
    for d in available_dates_raw
    if d.name.rstrip('/').isdigit() and len(d.name.rstrip('/')) == 8
])

if not available_dates_yyyymmdd:
    raise ValueError("❌ No snapshot dates found!")

# Convert to ISO format (YYYY-MM-DD) for dropdown and backfill compatibility
available_dates_iso = [
    f"{d[:4]}-{d[4:6]}-{d[6:]}" for d in available_dates_yyyymmdd
]

print(f"📅 Available Partitions: {available_dates_iso[0]} to {available_dates_iso[-1]} ({len(available_dates_iso)} dates)")

# Create dropdown widget with ISO dates (matches {{backfill.iso_date}} format)
dbutils.widgets.dropdown(
    "snapshot_date",
    available_dates_iso[-1],  # Default to latest
    available_dates_iso,      # ISO format dates
    "Snapshot Date"
)

# Get selected date and convert to YYYYMMDD for file paths
snapshot_date_iso = dbutils.widgets.get("snapshot_date")
snapshot_date = snapshot_date_iso.replace("-", "")  # YYYY-MM-DD → YYYYMMDD

print(f"✅ Selected: {snapshot_date_iso} (using {snapshot_date} for paths)")


# COMMAND ----------

path = f"/Volumes/interview_data_pde/app_insights_assignment/raw_apps/{snapshot_date}/*.json"
silver_table = "interview_data_pde.rbanyi_mecom.silver_apps_daily_snapshot"
gold_app_metrics_daily_table = "interview_data_pde.rbanyi_mecom.gold_app_metrics_daily"
gold_category_metrics_daily_table = "interview_data_pde.rbanyi_mecom.gold_category_metrics_daily"
quarantine_table = "interview_data_pde.rbanyi_mecom.bronze_apps_quarantine"

# COMMAND ----------

df_bronze = spark.read.json(path)
df_bronze = df_bronze.withColumn("snapshot_date", to_date(lit(snapshot_date_iso)))

print(f"✅ Loaded {df_bronze.count()} records from {path}")
df_bronze.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Quality - Duplicate Detection
# MAGIC
# MAGIC Detect and remove duplicate records by slug (natural key):
# MAGIC - Count duplicates by slug
# MAGIC - Keep record with lowest popularity (lower rank = more popular)
# MAGIC - Tie-breaker: highest internal_id
# MAGIC - ⚠️ Popularity is a RANK where lower is better (1 = most popular)

# COMMAND ----------

print("\n=== DUPLICATE DETECTION ===")

total_records = df_bronze.count()
# Count by slug (natural key)
duplicate_slugs = df_bronze.groupBy("slug").count().filter("count > 1")
duplicate_count = duplicate_slugs.count()

print(f"Total records: {total_records}")
print(f"Unique slugs: {df_bronze.select('slug').distinct().count()}")
print(f"Duplicate slugs: {duplicate_count}")

if duplicate_count > 0:
    print("\n⚠️  WARNING: Duplicate slugs found!")
    duplicate_slugs.orderBy(desc("count")).show(10)

    print("Applying deduplication by slug (keeping record with lowest popularity rank)")

    # Define window for deduplication (keep LOWEST popularity = highest rank)
    # Lower popularity number = more popular (it's a ranking: 1 = #1 app)
    window = Window.partitionBy("slug").orderBy(col("popularity").asc(), desc("internal_id"))

    df_bronze = df_bronze.withColumn("row_num", row_number().over(window)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")
else:
    print("✅ No duplicates found")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Quality - Metrics Validation
# MAGIC
# MAGIC Validate numeric metrics for data quality issues:
# MAGIC - Check for null values
# MAGIC - Detect negative values (invalid for counts/metrics)
# MAGIC - Count zero values
# MAGIC - Display statistical summary
# MAGIC - Capture records that fail quality checks for investigation (⚠️ this is for demonstration purposes)

# COMMAND ----------

# Define quarantine table

# Identify records with quality issues
df_quarantine = df_bronze.filter(
    col("slug").isNull() |
    col("popularity").isNull() | (col("popularity") < 0) |
    col("zap_usage_count").isNull() | (col("zap_usage_count") < 0) |
    col("request_count").isNull() | (col("request_count") < 0) |
    col("age_in_days").isNull() | (col("age_in_days") < 0) |
    col("days_since_last_update").isNull() | (col("days_since_last_update") < 0)
).withColumn("quarantine_reason", lit("Quality check failed")) \
 .withColumn("quarantine_timestamp", current_timestamp())

quarantine_count = df_quarantine.count()
print(f"\n⚠️  Quarantined {quarantine_count} records with quality issues")

if quarantine_count > 0:
    # Write quarantined records to table (idempotent - overwrite partition)
    df_quarantine.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("snapshot_date") \
        .option("partitionOverwriteMode", "dynamic") \
        .saveAsTable(quarantine_table)

    print(f"✅ Quarantined records saved to {quarantine_table} (dynamic overwrite)")

    # Show sample of quarantined records
    print("\nSample quarantined records:")
    df_quarantine.select("slug", "name", "popularity", "zap_usage_count", "quarantine_reason").show(5, truncate=False)
else:
    print("✅ No records quarantined - all passed quality checks")

# Filter out quarantined records from bronze
df_bronze_clean = df_bronze.filter(
    col("slug").isNotNull() &
    (col("popularity").isNotNull() & (col("popularity") >= 0)) &
    (col("zap_usage_count").isNotNull() & (col("zap_usage_count") >= 0)) &
    (col("request_count").isNotNull() & (col("request_count") >= 0)) &
    (col("age_in_days").isNotNull() & (col("age_in_days") >= 0)) &
    (col("days_since_last_update").isNotNull() & (col("days_since_last_update") >= 0))
)

clean_count = df_bronze_clean.count()
print(f"\n✅ Proceeding with {clean_count} clean records to silver layer")
print(f"   (Filtered out {quarantine_count} quarantined records)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer - Cleaned and Validated Data
# MAGIC
# MAGIC Apply transformations for clean, validated data:
# MAGIC - Generate deterministic surrogate key (SHA2 hash of slug + snapshot_date)
# MAGIC - Extract primary category from nested categories array
# MAGIC - Flatten nested structures into arrays
# MAGIC - Normalize boolean flags with defaults
# MAGIC - Handle nulls with appropriate coalesce defaults
# MAGIC - Add processing metadata

# COMMAND ----------

df_silver = df_bronze_clean.select(
    # Deterministic surrogate key
    sha2(
        concat_ws("||", col("slug"), col("snapshot_date")),
        256
    ).alias("app_snapshot_key"),

    # natural keys
    col("slug"),
    col("internal_id"),

    # Descriptive attributes
    col("name"),
    coalesce(col("description"), lit("")).alias("description"),
    coalesce(col("current_implementation_id"), lit("")).alias("current_implementation_id"),  # ← Fixed
    col("hashtag"),

    # Primary category (Denormalized for convenience)
    coalesce(
        when(size(col("categories")) > 0,
            array_sort(transform(col("categories"), lambda x: x["slug"]))[0]
        ),
        lit("uncategorized")
    ).alias("primary_category"),
    coalesce(
        when(size(col("categories")) > 0,
            array_sort(transform(col("categories"), lambda x: x["title"]))[0]
        ),
        lit("uncategorized")
    ).alias("primary_category_name"),
    transform(col("categories"), lambda x: x["title"]).alias("categories_title"),
    transform(col("categories"), lambda x: x["slug"]).alias("categories_slug"),


    # Degenerate Dimensions
    coalesce(col("is_premium"), lit(False)).alias("is_premium"),
    coalesce(col("is_featured"), lit(False)).alias("is_featured"),
    coalesce(col("is_beta"), lit(False)).alias("is_beta"),
    coalesce(col("is_upcoming"), lit(False)).alias("is_upcoming"),
    coalesce(col("is_public"), lit(True)).alias("is_public"),

    # Metrics (Measures)
    coalesce(col("popularity"), lit(0)).alias("popularity"),
    coalesce(col("zap_usage_count"), lit(0)).alias("zap_usage_count"),
    coalesce(col("request_count"), lit(0)).alias("request_count"),
    coalesce(col("age_in_days"), lit(0)).alias("age_in_days"),
    coalesce(col("days_since_last_update"), lit(0)).alias("days_since_last_update"),

    col("snapshot_date"),
    current_timestamp().alias("_processed_at")

)


# df_silver.display()

# COMMAND ----------

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("snapshot_date") \
    .option("overwriteSchema", "false") \
    .option("partitionOverwriteMode", "dynamic") \
    .saveAsTable(silver_table)

print(f"✅ Successfully wrote {df_silver.count()} records to {silver_table}")
print(f"✅ Partition: snapshot_date={snapshot_date_iso} (dynamic overwrite - other partitions preserved)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer - Aggregated Business Metrics
# MAGIC
# MAGIC ### Overview
# MAGIC
# MAGIC The gold layer contains aggregated metrics optimized for analytics and reporting:
# MAGIC - **Daily metrics**: App-level aggregations across all apps
# MAGIC - **Category metrics**: Category-level aggregations
# MAGIC

# COMMAND ----------

df_silver = spark.table(silver_table).filter(
    col("snapshot_date") == to_date(lit(snapshot_date_iso))
)

print(f"Silver records for {snapshot_date_iso}: {df_silver.count()}")


df_gold_daily = df_silver.groupBy("snapshot_date").agg(
    # Total counts
    count("*").alias("total_apps"),

    # Status breakdowns
    sql_sum(when(col("is_premium"), 1).otherwise(0)).alias("premium_apps"),
    sql_sum(when(~col("is_premium"), 1).otherwise(0)).alias("free_apps"),
    sql_sum(when(col("is_featured"), 1).otherwise(0)).alias("featured_apps"),
    sql_sum(when(col("is_beta"), 1).otherwise(0)).alias("beta_apps"),
    sql_sum(when(col("is_upcoming"), 1).otherwise(0)).alias("upcoming_apps"),

    # Freshness metrics
    sql_sum(when(col("days_since_last_update") <= 7, 1).otherwise(0)).alias("apps_updated_last_7d"),
    sql_sum(when(col("days_since_last_update") <= 30, 1).otherwise(0)).alias("apps_updated_last_30d"),
    sql_sum(when(col("days_since_last_update") <= 90, 1).otherwise(0)).alias("apps_updated_last_90d"),

    # Age distribution
    sql_sum(when(col("age_in_days") <= 30, 1).otherwise(0)).alias("apps_age_0_30d"),
    sql_sum(when((col("age_in_days") > 30) & (col("age_in_days") <= 365), 1).otherwise(0)).alias("apps_age_30d_1y"),
    sql_sum(when(col("age_in_days") > 365, 1).otherwise(0)).alias("apps_age_over_1y"),

    # Usage metrics (actual counts - these ARE additive)
    sql_sum("zap_usage_count").alias("total_zap_usage"),
    avg("zap_usage_count").alias("avg_zap_usage"),

    # Popularity rank distribution (lower rank = more popular)
    expr("MIN(popularity)").alias("best_popularity_rank"),  # Lowest number = most popular
    expr("MAX(popularity)").alias("worst_popularity_rank"),  # Highest number = least popular

    # Content quality
    sql_sum(when(length(col("description")) > 0, 1).otherwise(0)).alias("apps_with_description"),
    avg(length(col("description"))).alias("avg_description_length"),
    sql_sum(when(col("hashtag").isNotNull(), 1).otherwise(0)).alias("apps_with_hashtag"),

    # ETL metadata
    current_timestamp().alias("_processed_at")
)


# Show preview
# df_gold_daily.show(truncate=False)

# COMMAND ----------


df_gold_daily.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("snapshot_date") \
    .option("overwriteSchema", "false") \
    .option("partitionOverwriteMode", "dynamic") \
    .saveAsTable(gold_app_metrics_daily_table)

print(f"✅ Successfully wrote daily metrics to {gold_app_metrics_daily_table}")
print(f"✅ Partition: snapshot_date={snapshot_date_iso} (dynamic overwrite)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Category-Level Aggregated Metrics
# MAGIC
# MAGIC Aggregate metrics by primary category:
# MAGIC - App counts per category
# MAGIC - Status breakdowns (premium, featured, beta, upcoming)
# MAGIC - Popularity and usage metrics per category
# MAGIC - Age and freshness metrics per category
# MAGIC - Content quality per category

# COMMAND ----------

df_gold_categories = df_silver.groupBy("snapshot_date", "primary_category", "primary_category_name").agg(
    # Counts
    count("*").alias("app_count"),
    sql_sum(when(col("is_premium"), 1).otherwise(0)).alias("premium_count"),
    sql_sum(when(col("is_featured"), 1).otherwise(0)).alias("featured_count"),
    sql_sum(when(col("is_beta"), 1).otherwise(0)).alias("beta_count"),
    sql_sum(when(col("is_upcoming"), 1).otherwise(0)).alias("upcoming_count"),

    # Usage metrics (actual counts - additive)
    avg("zap_usage_count").alias("avg_zap_usage"),
    sql_sum("zap_usage_count").alias("total_zap_usage"),

    # Popularity rank distribution (lower = better, NOT additive)
    expr("MIN(popularity)").alias("best_popularity_rank"),
    expr("MAX(popularity)").alias("worst_popularity_rank"),

    # Age metrics
    avg("age_in_days").alias("avg_age_in_days"),
    avg("days_since_last_update").alias("avg_days_since_update"),

    # Content quality
    avg(length(col("description"))).alias("avg_description_length"),
    sql_sum(when(length(col("description")) > 0, 1).otherwise(0)).alias("apps_with_description"),

    # Freshness
    sql_sum(when(col("days_since_last_update") <= 30, 1).otherwise(0)).alias("apps_updated_last_30d"),

    # ETL metadata
    current_timestamp().alias("_processed_at")
).orderBy(col("app_count").desc())

# COMMAND ----------


# Write to Gold (idempotent partition overwrite)
df_gold_categories.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("snapshot_date") \
    .option("overwriteSchema", "false") \
    .option("partitionOverwriteMode", "dynamic") \
    .saveAsTable(gold_category_metrics_daily_table)

print(f"✅ Successfully wrote category metrics to {gold_category_metrics_daily_table}")
print(f"✅ Partition: snapshot_date={snapshot_date_iso} (dynamic overwrite)")
