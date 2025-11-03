"""Insights API endpoints."""

from datetime import date

from fastapi import APIRouter, HTTPException, Query

from ..config import get_settings
from ..database import db
from ..models import (
    AppHealthAssessmentResponse,
    AppMetricSnapshot,
    AppRiskItem,
    AppTimeSeriesResponse,
    CategoryInsight,
    PremiumAnalysisResponse,
    RiskSegment,
)

router = APIRouter(prefix="/insights", tags=["Insights"])
settings = get_settings()


def get_latest_snapshot(table_name: str) -> date:
    """
    Get the latest snapshot date from a table.

    Args:
        table_name: Full qualified table name

    Returns:
        Latest snapshot date

    Raises:
        HTTPException: If no data available
    """
    date_query = f"SELECT MAX(snapshot_date) as max_date FROM {table_name}"
    result = db.execute_query(date_query)

    if not result or result[0]["max_date"] is None:
        raise HTTPException(status_code=404, detail="No data available")

    return result[0]["max_date"]


@router.get("/premium-analysis", response_model=PremiumAnalysisResponse)
def get_premium_analysis(
    snapshot_date: date | None = Query(None, description="Specific snapshot date (defaults to latest)")
) -> PremiumAnalysisResponse:
    """
    Get premium vs free app analysis.

    Returns breakdown of premium/free apps with percentages.
    """
    # Get latest snapshot if not specified
    if snapshot_date is None:
        snapshot_date = get_latest_snapshot(settings.full_gold_daily_table)

    # Get premium analysis
    query = f"""
    SELECT
        snapshot_date,
        total_apps,
        premium_apps,
        free_apps,
        ROUND(100.0 * premium_apps / NULLIF(total_apps, 0), 2) as premium_percentage,
        featured_apps
    FROM {settings.full_gold_daily_table}
    WHERE snapshot_date = '{snapshot_date}'
    """

    results = db.execute_query(query)

    if not results:
        raise HTTPException(
            status_code=404, detail=f"No data found for snapshot_date={snapshot_date}"
        )

    return PremiumAnalysisResponse(**results[0])


@router.get("/categories", response_model=list[CategoryInsight])
def get_category_insights(
    snapshot_date: date | None = Query(None, description="Specific snapshot date"),
    min_app_count: int = Query(1, ge=1, description="Minimum app count filter"),
    limit: int = Query(20, ge=1, le=100, description="Maximum results to return"),
) -> list[CategoryInsight]:
    """
    Get category-level aggregated statistics.

    Returns metrics for each category including app counts and popularity rank distribution.
    Note: Popularity is a rank where lower numbers = more popular apps.
    """
    # Get latest snapshot if not specified
    if snapshot_date is None:
        snapshot_date = get_latest_snapshot(settings.full_gold_category_table)

    # Get category insights
    query = f"""
    SELECT
        primary_category as category_slug,
        primary_category_name as category_name,
        app_count,
        premium_count,
        featured_count,
        best_popularity_rank,
        worst_popularity_rank,
        ROUND(avg_age_in_days, 0) as avg_age_days
    FROM {settings.full_gold_category_table}
    WHERE snapshot_date = '{snapshot_date}'
      AND app_count >= {min_app_count}
    ORDER BY app_count DESC
    LIMIT {limit}
    """

    results = db.execute_query(query)

    if not results:
        raise HTTPException(
            status_code=404, detail=f"No data found for snapshot_date={snapshot_date}"
        )

    return [CategoryInsight(**row) for row in results]


@router.get("/app-health-assessment", response_model=AppHealthAssessmentResponse)
def get_app_health_assessment(
    snapshot_date: date | None = Query(None, description="Specific snapshot date"),
    limit_per_segment: int = Query(10, ge=1, le=50, description="Max apps per segment"),
) -> AppHealthAssessmentResponse:
    """
    Get app health and risk assessment with actionable segments.

    Segments apps into:
    - High Risk: Popular apps not updated in 90+ days
    - Rising Stars: New apps (< 180 days) with high engagement
    - Featured Underperformers: Featured apps below median popularity
    - Beta Graduation Ready: Beta apps with proven usage

    Enables Product team to:
    - Identify maintenance risks
    - Spot growth opportunities
    - Optimize featuring strategy
    - Promote beta apps to stable
    """
    # Get latest snapshot if not specified
    if snapshot_date is None:
        snapshot_date = get_latest_snapshot(settings.full_silver_table)

    # Single optimized CTE query - all segments in one database round trip
    query = f"""
    WITH stats AS (
        -- Calculate baseline statistics once
        SELECT
            COUNT(*) as total_apps,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY popularity) as median_popularity
        FROM {settings.full_silver_table}
        WHERE snapshot_date = '{snapshot_date}' AND popularity > 0
    ),
    high_risk AS (
        -- Popular apps not updated in 90+ days
        SELECT
            'high_risk' as segment,
            slug,
            name,
            primary_category as category,
            popularity,
            zap_usage_count,
            days_since_last_update,
            age_in_days, is_featured
        FROM {settings.full_silver_table}
        CROSS JOIN stats
        WHERE snapshot_date = '{snapshot_date}'
          AND days_since_last_update >= 90
          AND popularity < stats.median_popularity
        ORDER BY popularity ASC
        LIMIT {limit_per_segment}
    ),
    rising_stars AS (
        -- New apps (< 180 days) with high engagement
        SELECT
            'rising_stars' as segment,
            slug,
            name,
            primary_category as category,
            popularity,
            zap_usage_count,
            days_since_last_update,
            age_in_days,
            is_featured
        FROM {settings.full_silver_table}
        CROSS JOIN stats
        WHERE snapshot_date = '{snapshot_date}'
          AND age_in_days <= 180
          AND (popularity < stats.median_popularity OR zap_usage_count > 100)
        ORDER BY popularity ASC, zap_usage_count DESC
        LIMIT {limit_per_segment}
    ),
    featured_underperformers AS (
        -- Featured apps below median popularity
        SELECT
            'featured_underperformers' as segment,
            slug,
            name,
            primary_category as category,
            popularity,
            zap_usage_count,
            days_since_last_update,
            age_in_days, is_featured
        FROM {settings.full_silver_table}
        CROSS JOIN stats
        WHERE snapshot_date = '{snapshot_date}'
          AND is_featured = true
          AND popularity > stats.median_popularity
        ORDER BY popularity DESC
        LIMIT {limit_per_segment}
    ),
    beta_graduation AS (
        -- Beta apps with proven usage
        SELECT
            'beta_graduation' as segment,
            slug,
            name,
            primary_category as category,
            popularity,
            zap_usage_count,
            days_since_last_update,
            age_in_days,
            is_featured
        FROM {settings.full_silver_table}
        CROSS JOIN stats
        WHERE snapshot_date = '{snapshot_date}'
          AND is_beta = true
          AND age_in_days >= 90
          AND (popularity < stats.median_popularity OR zap_usage_count > 50)
        ORDER BY popularity ASC, zap_usage_count DESC
        LIMIT {limit_per_segment}
    ),
    all_segments AS (
        SELECT *, (SELECT total_apps FROM stats) as total_apps FROM high_risk
        UNION ALL
        SELECT *, (SELECT total_apps FROM stats) as total_apps FROM rising_stars
        UNION ALL
        SELECT *, (SELECT total_apps FROM stats) as total_apps FROM featured_underperformers
        UNION ALL
        SELECT *, (SELECT total_apps FROM stats) as total_apps FROM beta_graduation
    )
    SELECT * FROM all_segments
    """

    # Execute single optimized query
    results = db.execute_query(query)

    # Group results by segment
    segments: dict[str, list[dict]] = {
        "high_risk": [],
        "rising_stars": [],
        "featured_underperformers": [],
        "beta_graduation": [],
    }

    total_apps = 0
    for row in results:
        segment = row.pop("segment")
        total_apps = row.pop("total_apps")  # Same for all rows
        if segment in segments:
            segments[segment].append(row)

    # Build response
    return AppHealthAssessmentResponse(
        snapshot_date=snapshot_date,
        total_apps_analyzed=total_apps,
        high_risk_apps=RiskSegment(
            count=len(segments["high_risk"]),
            description="Popular apps not updated in 90+ days - maintenance risk",
            apps=[AppRiskItem(**row) for row in segments["high_risk"]],
        ),
        rising_stars=RiskSegment(
            count=len(segments["rising_stars"]),
            description="New apps (< 180 days) with high engagement - growth opportunity",
            apps=[AppRiskItem(**row) for row in segments["rising_stars"]],
        ),
        featured_underperformers=RiskSegment(
            count=len(segments["featured_underperformers"]),
            description="Featured apps below median popularity - review featuring strategy",
            apps=[AppRiskItem(**row) for row in segments["featured_underperformers"]],
        ),
        beta_graduation_ready=RiskSegment(
            count=len(segments["beta_graduation"]),
            description="Beta apps with proven usage - ready for promotion to stable",
            apps=[AppRiskItem(**row) for row in segments["beta_graduation"]],
        ),
    )


@router.get("/apps/{slug}/metrics", response_model=AppTimeSeriesResponse)
def get_app_metrics(
    slug: str,
    days: int = Query(30, ge=1, le=365, description="Number of days of historical data"),
) -> AppTimeSeriesResponse:
    """
    Get daily metrics for a specific app over time.

    Returns historical snapshots showing how the app's metrics evolved.
    Useful for tracking popularity trends, usage growth, and update frequency.
    """
    # Query silver table for app metrics across multiple snapshots
    query = f"""
    SELECT
        slug,
        name,
        primary_category,
        primary_category_name,
        is_premium,
        is_featured,
        snapshot_date,
        popularity,
        zap_usage_count,
        age_in_days,
        days_since_last_update
    FROM {settings.full_silver_table}
    WHERE slug = '{slug}'
    ORDER BY snapshot_date DESC
    LIMIT {days}
    """

    results = db.execute_query(query)

    if not results:
        raise HTTPException(
            status_code=404,
            detail=f"App '{slug}' not found in any snapshots"
        )

    # Extract app metadata from first (latest) record
    latest = results[0]

    # Build metrics list
    metrics = [
        AppMetricSnapshot(
            snapshot_date=row["snapshot_date"],
            popularity=row["popularity"],
            zap_usage_count=row["zap_usage_count"],
            age_in_days=row["age_in_days"],
            days_since_last_update=row["days_since_last_update"],
        )
        for row in results
    ]

    return AppTimeSeriesResponse(
        slug=latest["slug"],
        name=latest["name"],
        primary_category=latest["primary_category"],
        primary_category_name=latest["primary_category_name"],
        is_premium=latest["is_premium"],
        is_featured=latest["is_featured"],
        metrics=metrics,
    )
