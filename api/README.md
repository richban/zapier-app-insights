# Zapier App Insights API

Modern FastAPI service serving Zapier app catalog insights from Databricks.

## Features

- ✅ **Modern Python Stack**: Python 3.11+, FastAPI, Pydantic v2, uv package manager
- ✅ **Type Safety**: Full type hints with mypy strict mode
- ✅ **Structured Logging**: Using structlog for production-ready logging
- ✅ **Auto-generated Docs**: OpenAPI (Swagger) and ReDoc
- ✅ **Environment-based Config**: Pydantic Settings with .env support
- ✅ **Databricks Integration**: Direct SQL queries to Delta tables

## Quick Start

### Prerequisites

- Python 3.11+
- [uv](https://github.com/astral-sh/uv) package manager
- Databricks workspace access with personal access token

**Optional (Recommended):**
- [Nix](https://nixos.org/download.html) with flakes enabled
- [direnv](https://direnv.net/) for automatic environment loading

### Installation

#### Option 1: Using Nix + direnv (Recommended)

```bash
# Navigate to api directory
cd api

# Allow direnv (automatically loads Nix environment + .env)
direnv allow

# Install Python dependencies
uv sync

# Copy environment template and configure
cp .env.example .env
# Edit .env with your Databricks credentials
```

#### Option 2: Using uv only

```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Navigate to api directory
cd api

# Create virtual environment and install dependencies
uv sync

# Copy environment template and configure
cp .env.example .env
# Edit .env with your Databricks credentials
```

### Configuration

Update `.env` with your Databricks credentials:

```env
DATABRICKS_SERVER_HOSTNAME=dbc-a41d8d9a-943c.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
DATABRICKS_TOKEN=your_token_here
SCHEMA_NAME=your_schema  # e.g., rbanyi_mecom
```

### Running the API

```bash
# Development mode with auto-reload
uv run uvicorn zapier_insights.main:app --reload --port 8000

# Production mode
uv run uvicorn zapier_insights.main:app --host 0.0.0.0 --port 8000
```

### Access the API

- **Interactive Docs (Swagger)**: http://localhost:8000/docs
- **Alternative Docs (ReDoc)**: http://localhost:8000/redoc
- **Health Check**: http://localhost:8000/health
- **OpenAPI Schema**: http://localhost:8000/openapi.json

## API Endpoints

### Health Check

**GET** `/health`

Check API and database connectivity status.

```bash
curl http://localhost:8000/health
```

Response:
```json
{
  "status": "healthy",
  "timestamp": "2025-10-30T12:00:00Z",
  "latest_snapshot_date": "2025-10-30",
  "silver_row_count": 7234
}
```

### Premium Analysis

**GET** `/insights/premium-analysis`

Get premium vs free app breakdown with percentages.

**Query Parameters:**
- `snapshot_date` (optional): Specific snapshot date (defaults to latest)

```bash
curl http://localhost:8000/insights/premium-analysis
```

Response:
```json
{
  "snapshot_date": "2025-10-30",
  "total_apps": 7234,
  "premium_apps": 543,
  "free_apps": 6691,
  "premium_percentage": 7.51,
  "featured_apps": 124
}
```

### Category Insights

**GET** `/insights/categories`

Get category-level aggregated statistics.

**Query Parameters:**
- `snapshot_date` (optional): Specific snapshot date
- `min_app_count` (optional, default=1): Minimum apps per category
- `limit` (optional, default=20, max=100): Results limit

```bash
curl "http://localhost:8000/insights/categories?min_app_count=10&limit=5"
```

Response:
```json
[
  {
    "category_slug": "productivity",
    "category_name": "Productivity",
    "app_count": 892,
    "premium_count": 67,
    "featured_count": 23,
    "avg_popularity": 1245.5,
    "avg_age_days": 654
  }
]
```

### App Health Assessment

**GET** `/insights/app-health-assessment`

Get actionable app health and risk assessment with strategic segments.

**Query Parameters:**
- `snapshot_date` (optional): Specific snapshot date
- `limit_per_segment` (optional, default=10, max=50): Max apps per segment

```bash
curl "http://localhost:8000/insights/app-health-assessment?limit_per_segment=5"
```

Response:
```json
{
  "snapshot_date": "2025-10-30",
  "total_apps_analyzed": 7234,
  "high_risk_apps": {
    "count": 23,
    "description": "Popular apps not updated in 90+ days - maintenance risk",
    "apps": [
      {
        "slug": "legacy-app",
        "name": "Legacy CRM",
        "category": "crm",
        "popularity": 8500,
        "zap_usage_count": 12000,
        "days_since_last_update": 245,
        "age_in_days": 1825,
        "is_featured": true
      }
    ]
  },
  "rising_stars": {
    "count": 15,
    "description": "New apps (< 180 days) with high engagement - growth opportunity",
    "apps": [
      {
        "slug": "new-ai-tool",
        "name": "AI Assistant Pro",
        "category": "productivity",
        "popularity": 4200,
        "zap_usage_count": 850,
        "days_since_last_update": 5,
        "age_in_days": 120,
        "is_featured": false
      }
    ]
  },
  "featured_underperformers": {
    "count": 8,
    "description": "Featured apps below median popularity - review featuring strategy",
    "apps": [...]
  },
  "beta_graduation_ready": {
    "count": 12,
    "description": "Beta apps with proven usage - ready for promotion to stable",
    "apps": [...]
  }
}
```

**Product Team Actions:**
- **High Risk**: Contact partners for updates, evaluate deprecation
- **Rising Stars**: Invest in documentation, promote to users, consider featuring
- **Featured Underperformers**: Re-evaluate featuring criteria, remove low performers
- **Beta Graduation**: Promote to stable, add to marketing materials

## Development

### Running Tests

```bash
# Install dev dependencies
uv sync --all-extras

# Run tests
uv run pytest

# Run tests with coverage
uv run pytest --cov=zapier_insights --cov-report=html
```

### Code Quality

```bash
# Format code
uv run ruff format .

# Lint code
uv run ruff check .

# Type check
uv run mypy src/
```

### Project Structure

```
api/
├── src/zapier_insights/
│   ├── __init__.py
│   ├── main.py           # FastAPI application
│   ├── config.py         # Pydantic settings
│   ├── database.py       # Databricks connection
│   ├── models.py         # Pydantic models
│   └── routers/
│       ├── __init__.py
│       ├── health.py     # Health check endpoints
│       └── insights.py   # Insights endpoints
├── tests/                # Test suite
├── pyproject.toml        # Project config (uv)
├── .env.example          # Environment template
└── README.md
```

## Architecture

### Data Flow

```
Databricks (Bronze JSON)
    ↓
Silver Layer (Periodic Snapshot Fact Table)
    ↓
Gold Layer (Pre-aggregated Metrics) [Optional]
    ↓
FastAPI (REST Endpoints)
    ↓
Clients (JSON responses)
```

### Design Decisions

1. **uv for Package Management**: Faster than pip, built-in virtual env management
2. **Pydantic v2 Settings**: Type-safe configuration with environment variable support
3. **Structured Logging**: Machine-readable logs for production observability
4. **Connection-per-Request**: Simple pattern, sufficient for low-medium traffic
5. **Direct SQL Queries**: Leverages Databricks optimizations, no ORM overhead

## Production Considerations

### Not Implemented (MVP Scope)

- [ ] **Authentication**: API key auth, rate limiting
- [ ] **Connection Pooling**: Reduce per-request latency
- [ ] **Caching**: Redis for frequently accessed endpoints
- [ ] **Comprehensive Testing**: Unit + integration + load tests
- [ ] **Monitoring**: Prometheus metrics, Datadog APM
- [ ] **CI/CD**: GitHub Actions for automated testing/deployment
- [ ] **Error Tracking**: Sentry integration
- [ ] **Pagination**: Cursor-based for large result sets

### Production Requirements

1. **Security**:
   - API key authentication with per-user rate limits
   - Secrets management (AWS Secrets Manager / Vault)
   - SQL injection prevention (parameterized queries)

2. **Reliability**:
   - Connection pooling (reduce latency)
   - Circuit breaker pattern (handle DB failures)
   - Retry logic with exponential backoff
   - Health checks with DB connectivity validation

3. **Observability**:
   - Structured logging with correlation IDs
   - RED metrics (rate, errors, duration)
   - Distributed tracing (OpenTelemetry)
   - Alerting (PagerDuty integration)

4. **Performance**:
   - Query optimization (Z-ordering on Databricks)
   - Caching layer (Redis with TTL)
   - Response compression (gzip)
   - CDN for static docs

5. **Deployment**:
   - Containerized (Docker + Kubernetes)
   - Auto-scaling based on traffic
   - Blue-green deployment
   - Infrastructure as Code (Terraform)

## License

MIT
