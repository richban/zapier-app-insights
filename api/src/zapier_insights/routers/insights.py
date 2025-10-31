"""Insights API endpoints."""

import asyncio
from datetime import date

from fastapi import APIRouter, HTTPException, Query

from ..config import get_settings
from ..database import db
from ..models import (
    AppHealthAssessmentResponse,
    AppRiskItem,
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

    Returns metrics for each category including app counts and popularity.
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
        ROUND(avg_popularity, 2) as avg_popularity,
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
async def get_app_health_assessment(
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

    # First, get total apps and median popularity (needed for subsequent queries)
    total_query = f"""
    SELECT COUNT(*) as total FROM {settings.full_silver_table}
    WHERE snapshot_date = '{snapshot_date}'
    """

    median_query = f"""
    SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY popularity) as median_popularity
    FROM {settings.full_silver_table}
    WHERE snapshot_date = '{snapshot_date}' AND popularity > 0
    """

    # Run prerequisite queries in parallel
    total_result, median_result = await asyncio.gather(
        db.execute_query_async(total_query),
        db.execute_query_async(median_query),
    )

    total_apps = total_result[0]["total"] if total_result else 0
    median_popularity = median_result[0]["median_popularity"] if median_result else 0

    # Build all segment queries
    high_risk_query = f"""
    SELECT
        slug, name, primary_category as category, popularity, zap_usage_count,
        days_since_last_update, age_in_days, is_featured
    FROM {settings.full_silver_table}
    WHERE snapshot_date = '{snapshot_date}'
      AND days_since_last_update >= 90
      AND popularity > {median_popularity}
    ORDER BY popularity DESC
    LIMIT {limit_per_segment}
    """

    rising_stars_query = f"""
    SELECT
        slug, name, primary_category as category, popularity, zap_usage_count,
        days_since_last_update, age_in_days, is_featured
    FROM {settings.full_silver_table}
    WHERE snapshot_date = '{snapshot_date}'
      AND age_in_days <= 180
      AND (popularity > {median_popularity} OR zap_usage_count > 100)
    ORDER BY popularity DESC, zap_usage_count DESC
    LIMIT {limit_per_segment}
    """

    featured_underperformers_query = f"""
    SELECT
        slug, name, primary_category as category, popularity, zap_usage_count,
        days_since_last_update, age_in_days, is_featured
    FROM {settings.full_silver_table}
    WHERE snapshot_date = '{snapshot_date}'
      AND is_featured = true
      AND popularity < {median_popularity}
    ORDER BY popularity ASC
    LIMIT {limit_per_segment}
    """

    beta_graduation_query = f"""
    SELECT
        slug, name, primary_category as category, popularity, zap_usage_count,
        days_since_last_update, age_in_days, is_featured
    FROM {settings.full_silver_table}
    WHERE snapshot_date = '{snapshot_date}'
      AND is_beta = true
      AND age_in_days >= 90
      AND (popularity > {median_popularity * 0.5} OR zap_usage_count > 50)
    ORDER BY popularity DESC, zap_usage_count DESC
    LIMIT {limit_per_segment}
    """

    # Run all segment queries in parallel 🚀
    high_risk_results, rising_stars_results, featured_underperformers_results, beta_graduation_results = (
        await asyncio.gather(
            db.execute_query_async(high_risk_query),
            db.execute_query_async(rising_stars_query),
            db.execute_query_async(featured_underperformers_query),
            db.execute_query_async(beta_graduation_query),
        )
    )

    # Build response
    return AppHealthAssessmentResponse(
        snapshot_date=snapshot_date,
        total_apps_analyzed=total_apps,
        high_risk_apps=RiskSegment(
            count=len(high_risk_results),
            description="Popular apps not updated in 90+ days - maintenance risk",
            apps=[AppRiskItem(**row) for row in high_risk_results],
        ),
        rising_stars=RiskSegment(
            count=len(rising_stars_results),
            description="New apps (< 180 days) with high engagement - growth opportunity",
            apps=[AppRiskItem(**row) for row in rising_stars_results],
        ),
        featured_underperformers=RiskSegment(
            count=len(featured_underperformers_results),
            description="Featured apps below median popularity - review featuring strategy",
            apps=[AppRiskItem(**row) for row in featured_underperformers_results],
        ),
        beta_graduation_ready=RiskSegment(
            count=len(beta_graduation_results),
            description="Beta apps with proven usage - ready for promotion to stable",
            apps=[AppRiskItem(**row) for row in beta_graduation_results],
        ),
    )
