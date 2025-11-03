# Data Model Overview

This document provides a high-level overview of the Silver and Gold data models, designed to transform raw app data into actionable business insights.

## Silver Layer: The Single Source of Truth

The Silver layer contains cleaned, validated, and enriched data. It serves as the primary source for all downstream analysis and aggregated Gold tables.

### Table: `silver_apps_daily_snapshot`

This is the main table in our Silver layer. It contains one record for every app for each day we have data. Think of it as a complete, clean, daily catalog of all apps.

-   **Grain**: One record per App per Day.
-   **Purpose**: To provide a detailed, historical view of each app's attributes and metrics over time. It's the foundation for tracking trends for a specific app or building custom analyses.

#### Key Information Available:

| Column | Description | Business Question It Answers |
| :--- | :--- | :--- |
| `snapshot_date` | The date the data was captured. | "What did this app look like on a specific date?" |
| `slug` | The unique identifier for an app (e.g., "slack"). | "How can I find a specific app?" |
| `name` | The display name of the app (e.g., "Slack"). | "What is the name of this app?" |
| `primary_category_name` | The main category the app belongs to. | "What is the primary function of this app?" |
| `is_premium` | True if the app requires a paid Zapier plan. | "Is this a premium app?" |
| `is_featured` | True if Zapier is promoting this app. | "Is this a featured app?" |
| `popularity` | A rank where a lower number is better (1 is the best). | "How popular is this app compared to others?" |
| `zap_usage_count` | The number of active Zaps using this app. | "How much is this app being used?" |
| `age_in_days` | How many days ago the app was first added. | "Is this a new or established app?" |
| `days_since_last_update` | How many days ago the app was last updated. | "Is this app actively maintained?" |

---

## Gold Layer: Aggregated for Business Insights

The Gold layer contains pre-aggregated tables optimized for our API. These tables are designed for fast, efficient querying of key business metrics.

### Table 1: `gold_app_metrics_daily`

This table provides a daily summary of the entire app catalog.

-   **Grain**: One record per Day.
-   **Purpose**: To monitor high-level, daily trends and KPIs for the entire app ecosystem.

#### Key Information Available:

| Column | Description | Business Question It Answers |
| :--- | :--- | :--- |
| `snapshot_date` | The date the metrics were calculated for. | "What was the state of the catalog yesterday?" |
| `total_apps` | The total number of apps in the catalog. | "How many apps do we have in total?" |
| `premium_apps` | The count of apps requiring a paid plan. | "How many of our apps are premium?" |
| `featured_apps` | The count of apps being promoted. | "How many apps are we currently featuring?" |
| `apps_updated_last_30d` | The number of apps updated in the last 30 days. | "How many of our apps are actively maintained?" |
| `total_zap_usage` | The sum of all usage across all apps. | "What is the total engagement across the platform?" |
| `best_popularity_rank` | The rank of the most popular app (e.g., 1). | "What is the rank of our top app?" |

### Table 2: `gold_category_metrics_daily`

This table breaks down key metrics by app category for each day.

-   **Grain**: One record per Category per Day.
-   **Purpose**: To compare performance and composition across different app categories.

#### Key Information Available:

| Column | Description | Business Question It Answers |
| :--- | :--- | :--- |
| `snapshot_date` | The date the metrics were calculated for. | "What did the CRM category look like yesterday?" |
| `primary_category_name` | The name of the category (e.g., "CRM"). | "Which category are these metrics for?" |
| `app_count` | The total number of apps in this category. | "How many apps are in the CRM category?" |
| `premium_count` | The number of premium apps in this category. | "What is the number of premium apps in CRM?" |
| `avg_zap_usage` | The average usage count for apps in this category. | "How engaging are apps in this category on average?" |
| `best_popularity_rank` | The rank of the most popular app in this category. | "What is the top-ranked app in this category?" |
| `avg_days_since_update` | The average time since apps in this category were updated. | "Are apps in this category generally well-maintained?" |
