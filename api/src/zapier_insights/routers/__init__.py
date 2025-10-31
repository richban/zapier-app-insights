"""API routers."""

from .health import router as health_router
from .insights import router as insights_router

__all__ = ["health_router", "insights_router"]
