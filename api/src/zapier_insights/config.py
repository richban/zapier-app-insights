"""Application configuration using Pydantic Settings."""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Databricks connection
    databricks_server_hostname: str = Field(
        ..., description="Databricks workspace hostname"
    )
    databricks_http_path: str = Field(..., description="SQL warehouse HTTP path")
    databricks_token: str = Field(..., description="Personal access token")

    # Schema configuration
    catalog_name: str = Field(default="interview_data_pde", description="Databricks catalog")
    schema_name: str = Field(..., description="Your assigned schema (email_domain)")

    # Table names
    silver_table: str = Field(
        default="silver_apps_daily_snapshot", description="Silver layer fact table"
    )
    gold_daily_table: str = Field(
        default="gold_app_metrics_daily", description="Gold layer daily metrics"
    )
    gold_category_table: str = Field(
        default="gold_category_metrics_daily", description="Gold layer category metrics"
    )

    # API configuration
    api_title: str = Field(default="Zapier App Insights API", description="API title")
    api_version: str = Field(default="1.0.0", description="API version")
    api_description: str = Field(
        default="RESTful API serving Zapier app catalog insights",
        description="API description",
    )

    # Server configuration
    host: str = Field(default="0.0.0.0", description="Server host")
    port: int = Field(default=8000, description="Server port")
    reload: bool = Field(default=False, description="Auto-reload on code changes")

    # Logging
    log_level: str = Field(default="INFO", description="Logging level")

    @property
    def full_silver_table(self) -> str:
        """Full qualified silver table name."""
        return f"{self.catalog_name}.{self.schema_name}.{self.silver_table}"

    @property
    def full_gold_daily_table(self) -> str:
        """Full qualified gold daily table name."""
        return f"{self.catalog_name}.{self.schema_name}.{self.gold_daily_table}"

    @property
    def full_gold_category_table(self) -> str:
        """Full qualified gold category table name."""
        return f"{self.catalog_name}.{self.schema_name}.{self.gold_category_table}"


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
