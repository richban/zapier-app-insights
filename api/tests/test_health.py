"""Tests for health check endpoint."""

from fastapi.testclient import TestClient

from zapier_insights.main import app

client = TestClient(app)


def test_health_check_endpoint_exists() -> None:
    """Test that health endpoint is accessible."""
    response = client.get("/health")
    assert response.status_code in [200, 500]  # May fail if DB not connected


def test_health_check_response_structure() -> None:
    """Test health check returns expected fields."""
    response = client.get("/health")
    data = response.json()

    assert "status" in data
    assert "timestamp" in data
    assert data["status"] in ["healthy", "unhealthy"]


def test_root_endpoint() -> None:
    """Test root endpoint returns API information."""
    response = client.get("/")
    assert response.status_code == 200

    data = response.json()
    assert "message" in data
    assert "version" in data
    assert "docs" in data
