"""Databricks database connection and query execution."""

import asyncio
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

import structlog
from databricks import sql
from databricks.sql.client import Connection

from .config import get_settings

logger = structlog.get_logger(__name__)


class DatabaseService:
    """Databricks SQL connection manager and query executor."""

    def __init__(self) -> None:
        """Initialize database service with settings."""
        self.settings = get_settings()

    @contextmanager
    def get_connection(self) -> Generator[Connection, None, None]:
        """
        Context manager for database connections.

        Yields:
            Connection: Databricks SQL connection

        Raises:
            Exception: If connection fails
        """
        logger.info("establishing_databricks_connection")
        conn = sql.connect(
            server_hostname=self.settings.databricks_server_hostname,
            http_path=self.settings.databricks_http_path,
            access_token=self.settings.databricks_token,
            session_configuration={
                "query_tags": "source:insights-api,team:zapier,service:analytics"
            },
        )
        try:
            yield conn
        finally:
            logger.info("closing_databricks_connection")
            conn.close()

    def execute_query(self, query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """
        Execute SQL query and return results as list of dicts.

        Args:
            query: SQL query string
            params: Optional query parameters

        Returns:
            List of row dictionaries

        Raises:
            Exception: If query execution fails
        """
        logger.info("executing_query", query_preview=query[:100])

        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params or {})

                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    results = cursor.fetchall()
                    rows = [dict(zip(columns, row, strict=False)) for row in results]
                    logger.info("query_success", row_count=len(rows))
                    return rows

                logger.info("query_success_no_results")
                return []

    async def execute_query_async(
        self, query: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """
        Execute SQL query asynchronously (runs in thread pool to avoid blocking).

        Args:
            query: SQL query string
            params: Optional query parameters

        Returns:
            List of row dictionaries

        Raises:
            Exception: If query execution fails
        """
        # Run synchronous query in thread pool to avoid blocking event loop
        return await asyncio.to_thread(self.execute_query, query, params)


# Singleton instance
db = DatabaseService()
