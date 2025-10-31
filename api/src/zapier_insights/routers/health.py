"""Health check endpoints."""

from datetime import datetime, timezone

from fastapi import APIRouter

from ..config import get_settings
from ..database import db
from ..models import HealthResponse

router = APIRouter(tags=["Health"])
settings = get_settings()


@router.get("/health", response_model=HealthResponse)
def health_check() -> HealthResponse:
    """
    API health check with data freshness indicators.

    Returns system health status and latest snapshot information.
    """
    try:
        # Check database connectivity and get latest snapshot
        date_query = f"SELECT MAX(snapshot_date) as max_date FROM {settings.full_silver_table}"
        result = db.execute_query(date_query)

        latest_snapshot = result[0]["max_date"] if result and result[0]["max_date"] else None

        # Get row count for latest snapshot
        silver_count = None
        if latest_snapshot:
            count_query = f"""
            SELECT COUNT(*) as count
            FROM {settings.full_silver_table}
            WHERE snapshot_date = '{latest_snapshot}'
            """
            count_result = db.execute_query(count_query)
            silver_count = count_result[0]["count"] if count_result else None

        return HealthResponse(
            status="healthy",
            timestamp=datetime.now(timezone.utc),
            latest_snapshot_date=latest_snapshot,
            silver_row_count=silver_count,
        )

    except Exception as e:
        return HealthResponse(
            status="unhealthy",
            timestamp=datetime.now(timezone.utc),
            latest_snapshot_date=None,
            silver_row_count=None,
        )
