"""Pydantic models for request/response validation."""

from datetime import date, datetime

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    """Health check response."""

    status: str = Field(..., description="Health status: healthy or unhealthy")
    timestamp: datetime = Field(..., description="Check timestamp")
    latest_snapshot_date: date | None = Field(None, description="Most recent data snapshot")
    silver_row_count: int | None = Field(None, description="Silver table row count")


class PremiumAnalysisResponse(BaseModel):
    """Premium vs free app analysis."""

    snapshot_date: date = Field(..., description="Snapshot date")
    total_apps: int = Field(..., description="Total app count")
    premium_apps: int = Field(..., description="Premium app count")
    free_apps: int = Field(..., description="Free app count")
    premium_percentage: float = Field(..., description="Premium apps percentage")
    featured_apps: int = Field(..., description="Featured app count")


class CategoryInsight(BaseModel):
    """Category-level statistics."""

    category_slug: str = Field(..., description="Category identifier")
    category_name: str = Field(..., description="Category display name")
    app_count: int = Field(..., description="Total apps in category")
    premium_count: int = Field(..., description="Premium apps in category")
    featured_count: int = Field(..., description="Featured apps in category")
    best_popularity_rank: int = Field(..., description="Best (lowest) popularity rank in category")
    worst_popularity_rank: int = Field(..., description="Worst (highest) popularity rank in category")
    avg_age_days: float = Field(..., description="Average app age in days")


class AppSummary(BaseModel):
    """Lightweight app representation."""

    slug: str = Field(..., description="App identifier")
    name: str = Field(..., description="App display name")
    primary_category: str = Field(..., description="Primary category slug")
    primary_category_name: str = Field(..., description="Primary category name")
    is_premium: bool = Field(..., description="Premium app flag")
    is_featured: bool = Field(..., description="Featured app flag")
    popularity: int = Field(..., description="Popularity score")
    zap_usage_count: int = Field(..., description="Active zap count")


class AppRiskItem(BaseModel):
    """App in risk assessment."""

    slug: str = Field(..., description="App identifier")
    name: str = Field(..., description="App display name")
    category: str = Field(..., description="Primary category")
    popularity: int = Field(..., description="Popularity score")
    zap_usage_count: int = Field(..., description="Active zap count")
    days_since_last_update: int = Field(..., description="Days since last update")
    age_in_days: int = Field(..., description="App age in days")
    is_featured: bool = Field(..., description="Featured status")


class RiskSegment(BaseModel):
    """Risk assessment segment."""

    count: int = Field(..., description="Number of apps in segment")
    description: str = Field(..., description="Segment description")
    apps: list[AppRiskItem] = Field(..., description="Apps in this segment")


class AppHealthAssessmentResponse(BaseModel):
    """App health and risk assessment."""

    snapshot_date: date = Field(..., description="Snapshot date")
    total_apps_analyzed: int = Field(..., description="Total apps in analysis")

    high_risk_apps: RiskSegment = Field(
        ..., description="Popular apps not updated in 90+ days (maintenance risk)"
    )
    rising_stars: RiskSegment = Field(
        ..., description="New apps (< 180 days) with high engagement (growth opportunity)"
    )
    featured_underperformers: RiskSegment = Field(
        ..., description="Featured apps below median popularity (review featuring)"
    )
    beta_graduation_ready: RiskSegment = Field(
        ..., description="Beta apps with proven usage (promotion candidates)"
    )


class AppMetricSnapshot(BaseModel):
    """Single app metrics for a specific date."""

    snapshot_date: date = Field(..., description="Date of snapshot")
    popularity: int = Field(..., description="Popularity score")
    zap_usage_count: int = Field(..., description="Active zap count")
    age_in_days: int = Field(..., description="App age in days")
    days_since_last_update: int = Field(..., description="Days since last update")


class AppTimeSeriesResponse(BaseModel):
    """Historical metrics for a single app."""

    slug: str = Field(..., description="App identifier")
    name: str = Field(..., description="App display name")
    primary_category: str = Field(..., description="Primary category slug")
    primary_category_name: str = Field(..., description="Primary category name")
    is_premium: bool = Field(..., description="Premium status")
    is_featured: bool = Field(..., description="Featured status")
    metrics: list[AppMetricSnapshot] = Field(..., description="Daily metrics snapshots")


class ErrorResponse(BaseModel):
    """API error response."""

    error: str = Field(..., description="Error message")
    detail: str | None = Field(None, description="Detailed error information")
