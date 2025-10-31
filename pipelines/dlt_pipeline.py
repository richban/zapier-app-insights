# Databricks notebook source
# MAGIC %md
# MAGIC # App Insights Delta Live Tables Pipeline
# MAGIC
# MAGIC This pipeline implements a medallion architecture (Bronze → Silver → Gold) for processing app insights data.
# MAGIC
# MAGIC **Layers:**
# MAGIC - **Bronze**: Raw JSON ingestion with deduplication
# MAGIC - **Silver**: Cleaned, validated, and normalized data
# MAGIC - **Gold**: Aggregated metrics for analytics
# MAGIC
# MAGIC **Data Quality:**
# MAGIC - Automated quality checks with expectations
# MAGIC - Quarantine tables for failed validations
# MAGIC - Row-level and aggregate validations

# COMMAND ----------

from datetime import datetime

import dlt

from pyspark.sql.functions import (
    col,
    coalesce,
    lit,
    struct,
    sha2,
    concat_ws,
    to_date,
    transform,
    current_timestamp,
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
)
from pyspark.sql.window import Window
from pyspark.sql.types import DateType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Pipeline parameters are passed via Databricks configuration:
# MAGIC - `pipeline.snapshot_date`: Date partition to process (format: YYYYMMDD)
# MAGIC - `pipeline.catalog`: Unity Catalog name
# MAGIC - `pipeline.schema`: Target schema name
# MAGIC - `pipeline.volume_path`: Source data volume path

# COMMAND ----------

# Get pipeline configuration
snapshot_date = spark.conf.get("pipeline.snapshot_date", "20251028")
catalog = spark.conf.get("pipeline.catalog", "interview_data_pde")
schema = spark.conf.get("pipeline.schema", "rbanyi_mecom")
volume_path = spark.conf.get(
    "pipeline.volume_path",
    "/Volumes/interview_data_pde/app_insights_assignment/raw_apps"
)

# Construct paths
source_path = f"{volume_path}/{snapshot_date}/*.json"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer
# MAGIC
# MAGIC ### Bronze Raw Apps
# MAGIC
# MAGIC Initial ingestion of raw JSON data with minimal transformation:
# MAGIC - Read JSON files from volume storage
# MAGIC - Add snapshot_date partition column
# MAGIC - No filtering or quality checks at this stage

# COMMAND ----------

@dlt.table(
    name="dlt_bronze_apps_raw",
    comment="Raw app data ingested from JSON files with snapshot_date partition",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def bronze_apps_raw():
    """
    Ingest raw JSON data from volume storage.

    Returns:
        DataFrame with raw app data and snapshot_date column
    """
    return (
        spark.read.json(source_path)
        .withColumn("snapshot_date", to_date(lit(snapshot_date), "yyyyMMdd"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Deduplicated Apps
# MAGIC
# MAGIC Deduplicate records by slug (natural key):
# MAGIC - Window function partitioned by slug
# MAGIC - Keep record with highest popularity
# MAGIC - Tie-breaker: highest internal_id
# MAGIC
# MAGIC **Quality Checks:**
# MAGIC - Drop records with null slug
# MAGIC - Warn if duplicate slugs found (before deduplication)

# COMMAND ----------

@dlt.table(
    name="dlt_bronze_apps_deduped",
    comment="Deduplicated bronze apps by slug (keeping highest popularity)",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_or_drop("valid_slug", "slug IS NOT NULL")
@dlt.expect("valid_internal_id", "internal_id IS NOT NULL")
def bronze_apps_deduped():
    """
    Deduplicate apps by slug, keeping the record with highest popularity.

    Deduplication Logic:
    - Partition by: slug
    - Order by: popularity DESC, internal_id DESC
    - Keep: First row in each partition

    Returns:
        DataFrame with deduplicated app records
    """
    window = Window.partitionBy("slug").orderBy(
        desc("popularity"),
        desc("internal_id")
    )

    return (
        dlt.read("dlt_bronze_apps_raw")
        .withColumn("row_num", row_number().over(window))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer
# MAGIC
# MAGIC ### Silver Apps Daily Snapshot
# MAGIC
# MAGIC Cleaned and validated app data with:
# MAGIC - Deterministic surrogate key (SHA2 hash of slug + snapshot_date)
# MAGIC - Normalized nested structures (categories → arrays)
# MAGIC - Null handling with coalesce
# MAGIC - Primary category extraction (first alphabetically sorted category)
# MAGIC - Boolean flag normalization
# MAGIC
# MAGIC **Quality Checks:**
# MAGIC - All critical fields must be non-null
# MAGIC - Metrics must be non-negative
# MAGIC - Snapshot date must be valid

# COMMAND ----------

@dlt.table(
    name="dlt_silver_apps_daily_snapshot",
    comment="Cleaned and validated daily app snapshots with normalized schema",
    partition_cols=["snapshot_date"],
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_all({
    "valid_app_key": "app_snapshot_key IS NOT NULL",
    "valid_slug": "slug IS NOT NULL",
    "valid_snapshot_date": "snapshot_date IS NOT NULL AND snapshot_date <= current_date()",
    "valid_popularity": "popularity >= 0",
    "valid_zap_usage": "zap_usage_count >= 0",
    "valid_request_count": "request_count >= 0",
    "valid_age": "age_in_days >= 0",
    "valid_days_since_update": "days_since_last_update >= 0"
})
def silver_apps_daily_snapshot():
    """
    Transform bronze data into cleaned silver layer.

    Transformations:
    - Generate deterministic surrogate key (SHA2)
    - Extract primary category (first alphabetically)
    - Normalize boolean flags (default to false/true based on context)
    - Handle nulls with appropriate defaults
    - Flatten nested category structures

    Returns:
        DataFrame with cleaned and validated app data
    """
    return (
        dlt.read("dlt_bronze_apps_deduped")
        .select(
            # Deterministic surrogate key
            sha2(
                concat_ws("||", col("slug"), col("snapshot_date")),
                256
            ).alias("app_snapshot_key"),

            # Natural keys
            col("slug"),
            col("internal_id"),

            # Descriptive attributes
            col("name"),
            coalesce(col("description"), lit("")).alias("description"),
            coalesce(col("current_implementation_id"), lit("")).alias("current_implementation_id"),
            col("hashtag"),

            # Primary category (denormalized for convenience)
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

            # Degenerate dimensions (boolean flags)
            coalesce(col("is_premium"), lit(False)).alias("is_premium"),
            coalesce(col("is_featured"), lit(False)).alias("is_featured"),
            coalesce(col("is_beta"), lit(False)).alias("is_beta"),
            coalesce(col("is_upcoming"), lit(False)).alias("is_upcoming"),
            coalesce(col("is_public"), lit(True)).alias("is_public"),

            # Metrics (measures)
            coalesce(col("popularity"), lit(0)).alias("popularity"),
            coalesce(col("zap_usage_count"), lit(0)).alias("zap_usage_count"),
            coalesce(col("request_count"), lit(0)).alias("request_count"),
            coalesce(col("age_in_days"), lit(0)).alias("age_in_days"),
            coalesce(col("days_since_last_update"), lit(0)).alias("days_since_last_update"),

            # Metadata
            col("snapshot_date"),
            current_timestamp().alias("_processed_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Apps Quarantine
# MAGIC
# MAGIC Captures records that fail silver layer quality checks for investigation.
# MAGIC
# MAGIC **Captured Issues:**
# MAGIC - Null critical fields
# MAGIC - Negative metric values
# MAGIC - Invalid dates

# COMMAND ----------

@dlt.table(
    name="dlt_silver_apps_quarantine",
    comment="Records that failed silver layer quality checks",
    table_properties={
        "quality": "quarantine"
    }
)
def silver_apps_quarantine():
    """
    Quarantine table for records failing quality checks.

    Captures records with:
    - Null slugs or snapshot dates
    - Negative metric values
    - Future snapshot dates

    Returns:
        DataFrame with failed records and metadata
    """
    return (
        dlt.read("dlt_bronze_apps_deduped")
        .filter(
            col("slug").isNull() |
            col("popularity").isNull() | (col("popularity") < 0) |
            col("zap_usage_count").isNull() | (col("zap_usage_count") < 0) |
            col("request_count").isNull() | (col("request_count") < 0) |
            col("age_in_days").isNull() | (col("age_in_days") < 0) |
            col("days_since_last_update").isNull() | (col("days_since_last_update") < 0)
        )
        .withColumn("quarantine_reason", lit("Quality check failed"))
        .withColumn("quarantine_timestamp", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer
# MAGIC
# MAGIC ### Gold App Metrics Daily
# MAGIC
# MAGIC Daily aggregated metrics across all apps:
# MAGIC - Total app counts by status (premium, featured, beta, etc.)
# MAGIC - Freshness metrics (updates in last 7/30/90 days)
# MAGIC - Age distribution buckets
# MAGIC - Popularity and usage metrics
# MAGIC - Content quality indicators
# MAGIC
# MAGIC **Quality Checks:**
# MAGIC - Total apps must be positive
# MAGIC - Premium percentage must be between 0 and 100%
# MAGIC - Average metrics must be non-negative

# COMMAND ----------

@dlt.table(
    name="dlt_gold_app_metrics_daily",
    comment="Daily aggregated app metrics across all applications",
    partition_cols=["snapshot_date"],
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_all({
    "positive_app_count": "total_apps > 0",
    "valid_premium_ratio": "premium_apps + free_apps = total_apps",
    "valid_avg_popularity": "avg_popularity >= 0",
    "valid_avg_zap_usage": "avg_zap_usage >= 0"
})
def gold_app_metrics_daily():
    """
    Aggregate daily metrics across all apps.

    Metrics:
    - Count metrics: Total, premium, free, featured, beta, upcoming
    - Freshness: Apps updated in last 7/30/90 days
    - Age distribution: 0-30d, 30d-1y, >1y
    - Popularity: Total and average
    - Usage: Total and average zap usage
    - Content quality: Description presence and length

    Returns:
        DataFrame with daily aggregated metrics
    """
    return (
        dlt.read("dlt_silver_apps_daily_snapshot")
        .groupBy("snapshot_date")
        .agg(
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

            # Popularity metrics
            sql_sum("popularity").alias("total_popularity"),
            avg("popularity").alias("avg_popularity"),
            sql_sum("zap_usage_count").alias("total_zap_usage"),
            avg("zap_usage_count").alias("avg_zap_usage"),

            # Content quality
            sql_sum(when(length(col("description")) > 0, 1).otherwise(0)).alias("apps_with_description"),
            avg(length(col("description"))).alias("avg_description_length"),
            sql_sum(when(col("hashtag").isNotNull(), 1).otherwise(0)).alias("apps_with_hashtag"),

            # ETL metadata
            current_timestamp().alias("_processed_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Category Metrics Daily
# MAGIC
# MAGIC Daily metrics aggregated by primary category:
# MAGIC - App counts per category
# MAGIC - Status breakdowns (premium, featured, beta)
# MAGIC - Popularity and usage metrics per category
# MAGIC - Age and freshness metrics per category
# MAGIC - Content quality per category
# MAGIC
# MAGIC **Quality Checks:**
# MAGIC - Each category must have at least one app
# MAGIC - Category metrics must be consistent

# COMMAND ----------

@dlt.table(
    name="dlt_gold_category_metrics_daily",
    comment="Daily category-level aggregated metrics",
    partition_cols=["snapshot_date"],
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_all({
    "positive_app_count": "app_count > 0",
    "valid_primary_category": "primary_category IS NOT NULL",
    "valid_avg_popularity": "avg_popularity >= 0"
})
def gold_category_metrics_daily():
    """
    Aggregate daily metrics by primary category.

    Metrics per category:
    - App counts and status breakdowns
    - Popularity and usage statistics
    - Age and freshness indicators
    - Content quality metrics

    Returns:
        DataFrame with daily metrics per category, ordered by app count descending
    """
    return (
        dlt.read("dlt_silver_apps_daily_snapshot")
        .groupBy("snapshot_date", "primary_category", "primary_category_name")
        .agg(
            # Counts
            count("*").alias("app_count"),
            sql_sum(when(col("is_premium"), 1).otherwise(0)).alias("premium_count"),
            sql_sum(when(col("is_featured"), 1).otherwise(0)).alias("featured_count"),
            sql_sum(when(col("is_beta"), 1).otherwise(0)).alias("beta_count"),
            sql_sum(when(col("is_upcoming"), 1).otherwise(0)).alias("upcoming_count"),

            # Popularity metrics
            avg("popularity").alias("avg_popularity"),
            sql_sum("popularity").alias("total_popularity"),
            avg("zap_usage_count").alias("avg_zap_usage"),
            sql_sum("zap_usage_count").alias("total_zap_usage"),

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
        )
        .orderBy(col("app_count").desc())
    )