"""FastAPI application entrypoint."""

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .config import get_settings
from .routers import health_router, insights_router

# Configure structured logging
structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer(),
    ],
)

logger = structlog.get_logger(__name__)
settings = get_settings()


def create_app() -> FastAPI:
    """Create and configure FastAPI application."""
    app = FastAPI(
        title=settings.api_title,
        version=settings.api_version,
        description=settings.api_description,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )

    # CORS middleware (configure as needed)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # Restrict in production
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Include routers
    app.include_router(health_router)
    app.include_router(insights_router)

    @app.get("/")
    def root() -> dict[str, str]:
        """Root endpoint with API information."""
        return {
            "message": "Zapier App Insights API",
            "version": settings.api_version,
            "docs": "/docs",
            "health": "/health",
        }

    @app.on_event("startup")
    async def startup_event() -> None:
        """Log startup information."""
        logger.info(
            "api_starting",
            title=settings.api_title,
            version=settings.api_version,
            catalog=settings.catalog_name,
            schema=settings.schema_name,
        )

    @app.on_event("shutdown")
    async def shutdown_event() -> None:
        """Log shutdown information."""
        logger.info("api_shutting_down")

    return app


# Application instance
app = create_app()
